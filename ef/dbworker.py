import sqlalchemy
import time
import sys
import os
import shutil
import traceback
import yaml
from collections import deque, OrderedDict
from sqlalchemy.engine import reflection
from sqlalchemy.sql import select, update, insert
import Queue
from yaml import CLoader as Loader, CDumper as Dumper

class DBImportError(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return self.msg

class DBWorker(object):
    # In order to improve application performance and decrease the
    # number of spurious database operations, we batch updates
    # together before shipping them off to the database. An update
    # goes into self.write_cache initially, and any
    # further updates get applied to the object stored there. When the
    # timer ticks, we write out the queue to the database.

    def __init__(self, conn, result_queue):
        self.write_cache = OrderedDict()
        self.insert_queue = deque()
        self.conn = conn
        self.result_queue = result_queue
        self.meta = sqlalchemy.MetaData()
        self.meta.reflect(bind=self.conn)
        self.tables = self.meta.tables

    def post(self, op, result):
        #print 'worker posting', op, result
        try:
            self.result_queue.put((op, result))
        except Exception:
            self.post_exception()

    def post_exception(self):
        e = sys.exc_info()[1]
        msg = traceback.format_exc()
        self.post('exception', (e, msg))

    def task(self, task):
        #print 'worker task', task
        try:
            op, args = task
            if op == 'fetch_all':
                self.fetch_all(args)
            elif op == 'update':
                self.update(*args)
            elif op == 'upsert':
                self.upsert(*args)
            elif op == 'import':
                self.import_data(args)
            elif op == 'export':
                self.export_data(args)
        except Exception:
            self.post_exception()

    def shutdown(self):
        try:
            self.process_queues(-1)
        except Exception:
            self.post_exception()
        self.post('shutdown', None)

    def idle(self):
        try:
            self.process_queues(0.2)
        except Exception:
            self.post_exception()

    def process_queues(self, timeout):
        if timeout > 0:
            timer_end = time.time() + timeout
        else:
            timer_end = None

        def timed_out():
            if timer_end is None:
                return False
            return time.time() >= timer_end

        # Do all updates/inserts in batched transactions for (much) better performance
        batches = {}
        trans = self.conn.begin()
        try:
            while self.insert_queue and not timed_out():
                queued = self.insert_queue.popleft()
                self.do_insert(queued['table'], queued['values'], queued['origin'])
                for k,v in queued['batches'].iteritems():
                    batches[k] = v + batches.get(k, 0)

            while self.write_cache and not timed_out():
                k, queued = self.write_cache.popitem(last=False)
                table,key = k
                if queued['upsert']:
                    self.do_upsert(table, queued['values'], queued['origin'])
                else:
                    self.do_update(table, queued['values'], queued['origin'])
                for k,v in queued['batches'].iteritems():
                    batches[k] = v + batches.get(k, 0)
            trans.commit()
        except:
            exc_info = sys.exc_info()
            try:
                trans.rollback()
            except:
                pass
            raise exc_info[0], exc_info[1], exc_info[2]

        for k, v in batches.iteritems():
            self.post('batch_committed', (k,v))

        self.post('pending', len(self.write_cache) + len(self.insert_queue))

    def get_queued_update(self, table, key):
        new_rec = {'table': table, 'key_fields': None, 'values': {}, 'origin': set(), 'upsert': False, 'batches': {}}
        if key is None:
            self.insert_queue.append(new_rec)
            queued = new_rec
        else:
            k = (table, tuple(sorted(key.items())))
            queued = self.write_cache.setdefault(k, new_rec)
        return queued

    def fetch_all(self, table_name):
        table = self.tables[table_name]
        q = select([table])
        result = self.conn.execute(q)
        for row in iter(result.fetchone, None):
            values = dict(row.items())
            key = self.extract_key(table_name, values)
            self.post('fetch', {'table': table_name, 'key': key, 'values': values})
        self.post('fetch_all', table_name)

    def key_fields(self, table):
        return self.tables[table].primary_key.columns.keys()

    def value_fields(self, table):
        key_fields = set(self.key_fields(table))
        field_names = []
        for name in self.tables[table].c.keys():
            if name not in key_fields:
                field_names.append(name)
        return field_names

    def extract_key(self, table, values):
        key_fields = self.key_fields(table)
        try:
            key = dict([(k,values[k]) for k in key_fields])
            return key
        except KeyError:
            return None

    def update(self, table, values, origin, batchid):
        key = self.extract_key(table, values)
        if key is None:
            raise KeyError("update requires the key be fully specified")
        queued = self.get_queued_update(table, key)
        queued['values'].update(values)
        queued['batches'][batchid] = 1 + queued['batches'].get(batchid, 0)
        queued['origin'].add(origin)

    def upsert(self, table, values, origin, batchid):
        key = self.extract_key(table, values)
        queued = self.get_queued_update(table, key)
        queued['values'].update(values)
        queued['origin'].add(origin)
        queued['batches'][batchid] = 1 + queued['batches'].get(batchid, 0)
        queued['upsert'] = True

    def do_update(self, table_name, values, origin):
        value_fields = set(values) - set(self.key_fields(table_name))

        table = self.tables[table_name]

        q = table.update()
        for col in table.primary_key.columns:
            q = q.where(col == values[col.name])

        self.conn.execute(q.values(values))

        key = self.extract_key(table_name, values)
        self.post('update', {'table': table_name, 'key': key, 'values': values, 'origin': origin})

    def do_upsert(self, table_name, values, origin):
        key_fields = self.key_fields(table_name)
        table = self.tables[table_name]

        # Short-circuit: upsert with missing key goes right to the
        # database, and let the schema decide whether it's legitimate
        for k in key_fields:
            if k not in values:
                self.do_insert(table, values, origin)
                return

        q = select([table])
        for col in table.primary_key.columns:
            q = q.where(col == values[col.name])

        r = self.conn.execute(q).fetchone()

        if r is not None:
            self.do_update(table_name, values, origin)
        else:
            self.do_insert(table_name, values, origin)

    def do_insert(self, table_name, values, origin):
        key_fields = self.key_fields(table_name)
        table = self.tables[table_name]

        q = table.insert().values(values)
        r = self.conn.execute(q)

        key = self.extract_key(table_name, values)
        if key is None:
            key = dict()

        q = select([table])
        for col,val in zip(table.primary_key.columns, r.inserted_primary_key):
            q = q.where(col == val)

        r = self.conn.execute(q)
        row = r.fetchone()
        if row is not None:
            values = dict(row.items())
            key = self.extract_key(table_name, values)
            self.post('insert', {'table': table_name, 'key': key, 'values': values, 'origin': origin})
            return key
        else:
            return None

    def import_data(self, filename):
        try:
            f = open(filename)
            data = yaml.load(f, Loader=Loader)
            self.process_import(data)
        except Exception:
            msg = traceback.format_exc()
            print >>sys.stderr, msg
            self.post('import', msg)
        else:
            self.post('import', 'Imported OK')

    def export_data(self, filename):
        try:
            f = open(filename, 'w')
            data = self.prepare_export()
            f.write(yaml.dump(data, Dumper=Dumper))
        except Exception:
            msg = traceback.format_exc()
            print >>sys.stderr, msg
            self.post('export', msg)
        else:
            self.post('export', 'Exported OK')

    def process_import(self, data):
        if data.pop('$id', '') != 'ef-image-editor export':
            raise DBImportError('This does not look like a valid database export')

        trans = self.conn.begin()
        try:
            for table_name in data:
                print "Importing", table_name
                table = self.tables[table_name]
                if table_name == 'person':
                    for row in data[table_name]:
                        if not row.has_key('id'):
                            continue
                        q = select([table]).where(table.c.id == row['id'])
                        r = self.conn.execute(q).fetchone()
                    
                        if r is not None:
                            self.do_merge_person(r, row)
                        else:
                            self.do_insert(table_name, row, set(['import']))
                elif table_name == 'photo':
                    for row in data[table_name]:
                        if not row.has_key('url'):
                            continue
                        q = select([table]).where(table.c.url == row['url'])
                        r = self.conn.execute(q).fetchone()

                        if r is not None:
                            self.do_merge_photo(r, row)
                            photo_id = r['id']
                        else:
                            key = self.do_insert(table_name, row, set(['import']))
                            photo_id = key['id']
                        self.do_merge_current_photo(r['person_id'], photo_id)
                else:
                    for row in data[table_name]:
                        self.do_upsert(table_name, row, set(['import']))
            print "Committing..."
            trans.commit()
        except:
            exc_info = sys.exc_info()
            try:
                trans.rollback()
            except:
                pass
            raise exc_info[0], exc_info[1], exc_info[2]

        print "Done importing"

    def do_merge_photo(self, old, new):
        # url is the same, so fields relating to the photo are
        # presumed to be the same. Only edit fields should be
        # different
        if old['date_edited'] > new['date_edited']:
            # ...and we want to keep the edit fields from the copy
            # in the database, because that was edited more
            # recently
            return

        if old['date_edited'] == 0 and new['date_edited'] == 0:
            # Both records are from before the date_edited flag began
            # to be stored, or have no edits
            if new['opinion'] == 'unsure' and old['opinion'] != 'unsure':
                # The old record was changed from 'unsure' and the new
                # record has no opinion, so assume the old record was
                # edited
                return
        elif old['date_edited'] == new['date_edited']:
            # No edit happened, so don't waste time writing it
            return

        self.do_update('photo', new, set(['import']))

    def do_merge_person(self, old, new):
        if new['last_checked_at'] > old['last_checked_at']:
            self.do_update('person', new, set(['import']))

    def do_merge_current_photo(self, person_id, photo_id):
        person_table = self.tables['person']
        photo_table = self.tables['photo']
        person = self.conn.execute(select([person_table.c.current_photo_id]).where(person_table.c.id == person_id)).fetchone()
        if person is None:
            # Should never happen
            return

        current_photo_id = person['current_photo_id']
        if current_photo_id is not None:
            if photo_id == current_photo_id:
                # Nothing to do anyway if they're the same - this means we
                # just updated the current photo with new edits
                return
            current_photo = self.conn.execute(select([photo_table]).where(photo_table.c.id == current_photo_id)).fetchone()
            imported_photo = self.conn.execute(select([photo_table]).where(photo_table.c.id == photo_id)).fetchone()
            if imported_photo is None:
                # Should never happen
                return
            if current_photo is not None:
                if current_photo['date_fetched'] > imported_photo['date_fetched']:
                    # The imported photo is older, so stop here
                    return

        # The imported photo should become the current photo - it's
        # more recent. Edits don't matter if we pulled a new photo
        # from eventsforce.
        self.do_update('person', {'id': person_id, 'current_photo_id': photo_id}, set(['import']))

    def prepare_export(self):
        data = {'$id': 'ef-image-editor export'}

        for table_name in self.tables:
            rows = data[table_name] = []
            table = self.tables[table_name]
            q = select([table])
            result = self.conn.execute(q)
            for row in iter(result.fetchone, None):
                rows.append(dict(row.items()))

        return data

def setup_session(datadir):
    dbfile = os.path.join(datadir, 'database.sqlite')

    for i in reversed(xrange(0,9)):
        f1 = '%s.%d' % (dbfile, i)
        f2 = '%s.%d' % (dbfile, i+1)
        if os.path.exists(f2):
            os.remove(f2)
        if os.path.exists(f1):
            os.rename(f1, f2)
    if os.path.exists(dbfile):
        shutil.copy(dbfile, '%s.0' % dbfile)

    engine = sqlalchemy.create_engine('sqlite:///' + dbfile)
    conn = engine.connect()
    
    conn.execute('vacuum')

    insp = reflection.Inspector.from_engine(conn)
    tables = insp.get_table_names()

    if 'person' not in tables:
        conn.execute("""CREATE TABLE person (
                       id INTEGER NOT NULL,
                       firstname VARCHAR,
                       lastname VARCHAR,
                       title VARCHAR,
                       fullname VARCHAR,
                       police_status varchar,
                       current_photo_id INTEGER,
                       last_checked_at float,
                       PRIMARY KEY (id)
                       )""")
    else:
        columns = dict([ (col['name'], col) for col in insp.get_columns('person') ])
        if not 'police_status' in columns:
            conn.execute("""alter table person add column police_status varchar""")
    if 'photo' not in tables:
        conn.execute('''CREATE TABLE photo (
                       id INTEGER NOT NULL,
                       url VARCHAR,
                       filename varchar,
                       date_fetched float,
                       date_edited float default 0.0,
                       uploaded boolean default 0,
                       person_id INTEGER,
                       width integer default 0,
                       height integer default 0,
                       crop_centre_x FLOAT default 0.5,
                       crop_centre_y FLOAT default 0.5,
                       crop_scale FLOAT default 1.0,
                       brightness float default 0.0,
                       contrast float default 0.0,
                       gamma float default 1.0,
                       rotate FLOAT default 0,
                       block_upload boolean default 0,
                       load_failed boolean default 0,
                       opinion VARCHAR(6) default 'unsure',
                       PRIMARY KEY (id),
                       UNIQUE(url, person_id),
                       FOREIGN KEY(person_id) REFERENCES person (id),
                       CONSTRAINT photo_opinion CHECK (opinion IN ('ok', 'bad', 'unsure'))
                       )''')
    else:
        columns = dict([ (col['name'], col) for col in insp.get_columns('photo') ])
        if not columns.has_key('filename'):
            conn.execute('''alter table photo add column filename varchar''')
        if not columns.has_key('width'):
            conn.execute('''alter table photo add column width integer default 0''')
        if not columns.has_key('height'):
            conn.execute('''alter table photo add column height integer default 0''')
        if not columns.has_key('brightness'):
            conn.execute('''alter table photo add column brightness float default 0.0''')
        if not columns.has_key('contrast'):
            conn.execute('''alter table photo add column contrast float default 0.0''')
        if not columns.has_key('gamma'):
            conn.execute('''alter table photo add column gamma float default 1.0''')
        if not columns.has_key('block_upload'):
            conn.execute('''alter table photo add column block_upload boolean default 0''')
        if not columns.has_key('load_failed'):
            conn.execute('''alter table photo add column load_failed boolean default 0''')
        if not columns.has_key('uploaded'):
            conn.execute('''alter table photo add column uploaded boolean default 0''')
        if not columns.has_key('date_edited'):
            conn.execute('''alter table photo add column date_edited float default 0.0''')

    if 'event' not in tables:
        conn.execute('''CREATE TABLE event (
	               id INTEGER NOT NULL,
                       name VARCHAR,
                       PRIMARY KEY (id)
                       )''')
    if 'registration' not in tables:
        conn.execute('''CREATE TABLE registration (
                       person_id INTEGER NOT NULL,
                       event_id INTEGER NOT NULL,
                       attendee_type VARCHAR,
                       booking_ref VARCHAR,
                       booker_email VARCHAR,
                       booker_firstname VARCHAR,
                       booker_lastname VARCHAR,
                       PRIMARY KEY (person_id, event_id),
                       FOREIGN KEY(person_id) REFERENCES person (id),
                       FOREIGN KEY(event_id) REFERENCES event (id)
                       )''')

    return conn

def start_dbworker(write_queue, result_queue, datadir):
    conn = setup_session(datadir)
    worker = DBWorker(conn, result_queue)

    idle_timeout = 0.3

    while True:
        timer_end = time.time() + idle_timeout
        while time.time() < timer_end:
            try:
                task = write_queue.get(True, 0.1)
                if task == 'STOP':
                    worker.shutdown()
                    return
                else:
                    worker.task(task)
            except Queue.Empty:
                pass
        worker.idle()
