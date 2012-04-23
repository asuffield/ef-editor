import os
import weakref
import time
import traceback
import shutil
from PyQt4 import QtCore, QtSql
from ef.threads import WorkerThread
from collections import deque, OrderedDict
from ef.task import Finishable
from ef.lib import LRUCache, SignalGroup

photodir = None

# We can't pass a signal directly into the db worker thread, because
# there's no apparent way to put pyqtBoundSignal into a type
# signature. So, we pass these proxy objects around and chain
# connections through them
class DBBaseProxy(QtCore.QObject):
    # XXX: Should probably make the origin a set instead of a string
    update = QtCore.pyqtSignal(dict, str)
    # XXX: Should also have a signal for objects which are not found in the database

class BatchProxy(QtCore.QObject):
    commit = QtCore.pyqtSignal(int)

class DBException(Exception):
    def __init__(self, msg, query):
        self.msg = msg
        self.error = query.lastError().text()
    def __str__(self):
        return "%s: %s" % (self.msg, self.error)

class DBWorker(QtCore.QObject):
    # XXX: Should map types rather than using QVariant in the dicts here
    created = QtCore.pyqtSignal(str, dict, str)
    updated = QtCore.pyqtSignal(str, dict, str)
    pending = QtCore.pyqtSignal(int)

    # In order to improve application performance and decrease the
    # number of spurious database operations, we batch updates
    # together before shipping them off to the database. An update
    # goes into self.write_cache initially, and any
    # further updates get applied to the object stored there. When the
    # timer ticks, we write out the queue to the database.

    def __init__(self):
        QtCore.QObject.__init__(self)

        self.binds = {}
        self.batches = {}
        self.tables = {}
        self.write_cache = OrderedDict()
        self.insert_queue = deque()

    @QtCore.pyqtSlot(dict)
    def register_class(self, rec):
        self.tables[rec['table']] = rec

    def class_def(self, table):
        k = str(table)
        if not self.tables.has_key(k):
            raise Exception("Table %s does not have a class registered" % k)
        return self.tables[k]

    @QtCore.pyqtSlot()
    def setup(self):
        self.ticker = QtCore.QTimer(self)
        self.ticker.setInterval(300)
        self.ticker.timeout.connect(self.timeout)
        self.ticker.start()

    @QtCore.pyqtSlot(int)
    def exit(self, rc):
        self.process_queues(-1)
        QtCore.QThread.currentThread().exit(rc)

    def timeout(self):
        self.process_queues(200)

    def process_queues(self, timeout):
        timer = QtCore.QElapsedTimer()
        timer.start()

        # Do all updates/inserts in batched transactions for (much) better performance
        batches = {}
        db = QtSql.QSqlDatabase.database()
        db.transaction()
        try:
            while self.insert_queue and not timer.hasExpired(timeout):
                queued = self.insert_queue.popleft()
                self.do_insert(queued['table'], queued['values'], queued['origin'])
                for k,v in queued['batches'].iteritems():
                    batches[k] = v + batches.get(k, 0)
                
            while self.write_cache and not timer.hasExpired(timeout):
                k, queued = self.write_cache.popitem(last=False)
                table,key = k
                if queued['upsert']:
                    self.do_upsert(table, queued['values'], queued['origin'])
                else:
                    self.do_update(table, queued['values'], queued['origin'])
                for k,v in queued['batches'].iteritems():
                    batches[k] = v + batches.get(k, 0)
            db.commit()
        except:
            db.rollback()
            raise

        for k, v in batches.iteritems():
            if self.batches.has_key(k):
                proxy = self.batches[k]['proxy']()
                if proxy is None:
                    del self.batches[k]
                else:
                    proxy.commit.emit(v)

        # Remove all invalidated weak references
        for x in self.binds.itervalues():
            for bindings in x.itervalues():
                bindings = filter(lambda binding: binding['proxy']() is not None, bindings)

        self.pending.emit(len(self.write_cache))

    def get_binds(self, table, key):
        k = tuple(sorted(key.items()))
        return self.binds.setdefault(str(table), {}).setdefault(k, [])

    def get_queued_update(self, table, key):
        new_rec = {'table': table, 'key_fields': None, 'values': {}, 'origin': set(), 'upsert': False, 'batches': {}}
        if key is None:
            self.insert_queue.append(new_rec)
            queued = new_rec
        else:
            k = (table, tuple(sorted(key.items())))
            queued = self.write_cache.setdefault(k, new_rec)
        self.pending.emit(len(self.write_cache) + len(self.insert_queue))
        return queued

    @QtCore.pyqtSlot(str, dict, DBBaseProxy)
    def bind(self, table, key, proxy):
        binding = {'proxy': weakref.ref(proxy)}
        self.get_binds(table, key).append(binding)
        self.fetch(table, key, binding)

    @QtCore.pyqtSlot(int, BatchProxy)
    def register_batch(self, key, proxy):
        self.batches[key] = {'proxy': weakref.ref(proxy)}

    def key_expr(self, table):
        key_fields = self.class_def(table)['key']
        return ' and '.join(map(lambda k: '%s = :%s' % (k,k), key_fields))

    def key_fields(self, table):
        return self.class_def(table)['key']

    def value_fields(self, table):
        key_fields = self.key_fields(table)
        field_names = []
        for name in self.class_def(table)['fields']:
            if name not in key_fields:
                field_names.append(name)
        return field_names

    def fetch(self, table, key, binding):
        field_names = self.value_fields(table)
        
        query = QtSql.QSqlQuery()
        if not query.prepare('select %s from %s where %s' % (','.join(field_names), table, self.key_expr(table))):
            raise DBException("Prepare failed", query)
        for k in self.key_fields(table):
            query.bindValue(':%s' % k, key[k])
        query.setForwardOnly(True)
        if not query.exec_():
            raise DBException("Query failed?", query)

        if query.next():
            results = {}
            for i, k in enumerate(field_names):
                results[k] = query.value(i)
            proxy = binding['proxy']()
            if proxy is not None:
                proxy.update.emit(results, 'bind')

        query.finish()

    # This should probably just be something that happens automatically when the thread starts...
    @QtCore.pyqtSlot(str)
    def signal_existing_created(self, table):
        key_fields = self.key_fields(table)

        query = QtSql.QSqlQuery()
        if not query.prepare('select %s from %s' % (','.join(key_fields), table)):
            raise DBException('Prepare failed', query)
        query.exec_()
        while query.next():
            values = map(lambda i: query.value(i), xrange(0, len(key_fields)))
            key = dict(zip(key_fields, values))
            self.created.emit(table, key, 'signal_existing')
        query.finish()

    def extract_key(self, table, values):
        key_fields = self.key_fields(table)
        try:
            key = dict([(k,values[k]) for k in key_fields])
            return key
        except KeyError:
            return None

    @QtCore.pyqtSlot(str, dict, str, int)
    def update(self, table, values, origin, batchid):
        key = self.extract_key(table, values)
        if key is None:
            raise KeyError("update requires the key be fully specified")
        queued = self.get_queued_update(table, key)
        queued['values'].update(values)
        queued['batches'][batchid] = 1 + queued['batches'].get(batchid, 0)
        queued['origin'].add(origin)

    @QtCore.pyqtSlot(str, dict, str, int)
    def upsert(self, table, values, origin, batchid):
        key = self.extract_key(table, values)
        queued = self.get_queued_update(table, key)
        queued['values'].update(values)
        queued['origin'].add(origin)
        queued['batches'][batchid] = 1 + queued['batches'].get(batchid, 0)
        queued['upsert'] = True

    def do_update(self, table, values, origin):
        value_fields = set(values) - set(self.key_fields(table))

        query = QtSql.QSqlQuery()
        query.prepare('update %s set %s where %s' % (table, ','.join(map(lambda k: "%s = :%s" % (k,k), value_fields)), self.key_expr(table)))
        for k, v in values.iteritems():
            query.bindValue(':%s' % k, v)
        query.setForwardOnly(True)
        if not query.exec_():
            raise DBException("Query failed!?", query)
        query.finish()

        key = self.extract_key(table, values)

        for binding in self.get_binds(table, key):
            proxy = binding['proxy']()
            if proxy is not None:
                for o in origin:
                    proxy.update.emit(values, o)

        for o in origin:
            self.updated.emit(table, key, o)

    def do_upsert(self, table, values, origin):
        key_fields = self.key_fields(table)

        # Short-circuit: upsert with missing key goes right to the
        # database, and let the schema decide whether it's legitimate
        for k in key_fields:
            if k not in values:
                self.do_insert(table, values, origin)
                return

        query = QtSql.QSqlQuery()
        stmt = 'select %s from %s where %s' % (','.join(key_fields), table, self.key_expr(table))
        if not query.prepare(stmt):
            raise DBException("Prepare failed!", query)
        for k in key_fields:
            query.bindValue(':%s' % k, values[k])
        query.setForwardOnly(True)
        if not query.exec_():
            raise DBException("Query failed!", query)
        if query.next():
            self.do_update(table, values, origin)
        else:
            self.do_insert(table, values, origin)
        query.finish()

    def do_insert(self, table, values, origin):
        key_fields = self.key_fields(table)

        query = QtSql.QSqlQuery()
        keys = values.keys()
        if not query.prepare('insert into %s (%s) values (%s)' % (table, ','.join(keys), ','.join(map(lambda k: ':' + k, keys)))):
            raise DBException("Prepare failed!", query)
        for k, v in values.iteritems():
            query.bindValue(':%s' % k, v)
        query.setForwardOnly(True)
        if not query.exec_():
            raise DBException("Query failed?!", query)
        query.finish()

        query = QtSql.QSqlQuery('select %s from %s where rowid = last_insert_rowid()' % (','.join(key_fields), table))
        if not query.next():
            raise DBException("Failed to find row just inserted?!", query)

        key = dict([(key_fields[i],query.value(i)) for i in xrange(0, len(key_fields))])
        for o in origin:
            self.created.emit(table, key, o)

class DBManager(QtCore.QObject):
    sig_bind = QtCore.pyqtSignal(str, dict, DBBaseProxy)
    sig_update = QtCore.pyqtSignal(str, dict, str, int)
    sig_upsert = QtCore.pyqtSignal(str, dict, str, int)
    sig_register_batch = QtCore.pyqtSignal(int, BatchProxy)
    sig_register_class = QtCore.pyqtSignal(dict)
    sig_signal_existing_created = QtCore.pyqtSignal(str)

    created = QtCore.pyqtSignal(str, dict)
    updated = QtCore.pyqtSignal(str, dict)

    def __init__(self):
        super(QtCore.QObject, self).__init__()

        self.worker = WorkerThread()
        self.dbworker = DBWorker()
        self.dbworker.moveToThread(self.worker)

        self.sig_bind.connect(self.dbworker.bind)
        self.sig_update.connect(self.dbworker.update)
        self.sig_upsert.connect(self.dbworker.upsert)
        self.sig_register_batch.connect(self.dbworker.register_batch)
        self.sig_register_class.connect(self.dbworker.register_class)
        self.sig_signal_existing_created.connect(self.dbworker.signal_existing_created)
        self.dbworker.updated.connect(self.updated)
        self.dbworker.created.connect(self.created)
        self.dbworker.pending.connect(self.handle_pending)
        self.worker.started.connect(self.dbworker.setup)
        self.worker.please_exit.connect(self.dbworker.exit)

        self.pending_op_count = 0
        self.classes = {}

    def start(self):
        self.worker.start()
        for rec in self.classes.itervalues():
            self.sig_register_class.emit(rec)

    def handle_pending(self, count):
        self.pending_op_count = count

    def pending(self):
        return self.pending_op_count

    # Wrappers to hide the Qt noise

    def bind(self, table, key, proxy):
        self.sig_bind.emit(table, key, proxy)

    def register_batch(self, key, batch):
        self.sig_register_batch.emit(key, batch)

    def register_class(self, dbclass, name):
        rec = {'name': name,
               'table': dbclass.__tablename__,
               'key': dbclass.__key__,
               'fields': dbclass.__fields__,
               }
        if self.classes.has_key(rec['table']):
            raise Exception("Multiple classes defined for table %s (other is %s)" % (rec['table'], self.classes[rec['table']]['name']))
        self.classes[rec['table']] = rec
        if self.worker.isRunning():
            self.sig_register_class.emit(rec)

    def update(self, table, values, origin, batchid):
        self.sig_update.emit(table, values, origin, batchid)

    def upsert(self, table, values, origin, batchid):
        self.sig_upsert.emit(table, values, origin, batchid)

    def signal_existing_created(self, table):
        self.sig_signal_existing_created.emit(table)

dbmanager = DBManager()

class DBBaseMeta(type(QtCore.QObject)):
    def __init__(self, name, bases, dict):
        super(DBBaseMeta, self).__init__(name, bases, dict)
        if not hasattr(self, '__tablename__'):
            return

        dbmanager.register_class(self, name)

class DBBase(QtCore.QObject, Finishable):
    __metaclass__ = DBBaseMeta
    updated = QtCore.pyqtSignal(str)

    def __init__(self, *key_values, **kwargs):
        QtCore.QObject.__init__(self)
        Finishable.__init__(self, self.updated)

        key_fields = self.__key__
        key_dict = kwargs.pop('key', None)

        object.__setattr__(self, '__key__', {})
        object.__setattr__(self, '__values__', {})
        values = self.__values__
        key = self.__key__
        
        for k in self.__fields__:
            values[k] = None

        if key_dict is not None:
            key_values = [key_dict[k] for k in key_fields]

        for k, v in zip(key_fields, key_values):
            values[k] = v
            key[k] = v

        object.__setattr__(self, '__proxy__', DBBaseProxy())
        # proxies don't have any methods so this doesn't really matter...
        self.__proxy__.moveToThread(dbmanager.worker)
        self.__proxy__.update.connect(self._do_update)
        dbmanager.bind(self.__tablename__, key, self.__proxy__)

    def __getattr__(self, name):
        return self.__values__[name]

    def __getitem__(self, name):
        return self.__values__[name]

    def __iter__(self):
        return self.__values__.iterkeys()

    def iterkeys(self):
        return self.__values__.iterkeys()

    def itervalues(self):
        return self.__values__.itervalues()

    def iteritems(self):
        return self.__values__.iteritems()

    def __contains__(self, name):
        return name in self.__values__

    def _do_update(self, data, origin):
        for k,v in data.iteritems():
            try:
                if isinstance(v, QtCore.QVariant):
                    v = self.__fields__[k](v)
                setattr(self, k, v)
            except TypeError:
                setattr(self, k, None)
        self.updated.emit(origin)

    @classmethod
    def _check_values(self, values):
        for k in values:
            if k not in self.__fields__:
                raise KeyError("Invalid key %s in values" % k)
        for k in self.__key__:
            if k not in values:
                raise KeyError("Key field %s missing from values" % k)

    @classmethod
    def batch_op(self, batch):
        if batch is None:
            return 0
        else:
            batch.add_op()
            return id(batch)

    # Update does not require the key to be passed (unlike upsert),
    # because it takes that from the instance
    def update(self, values={}, origin='', batch=None, **kwargs):
        self._check_values(values)
        values = dict(values)
        values.update(kwargs)
        values.update(self.__key__)
        dbmanager.update(self.__tablename__, values, origin, self.batch_op(batch))

    @classmethod
    def upsert(self, values={}, origin='', batch=None, **kwargs):
        self._check_values(values)
        values = dict(values)
        values.update(kwargs)
        dbmanager.upsert(self.__tablename__, values, origin, self.batch_op(batch))

    @classmethod
    def signal_existing_created(self):
        dbmanager.signal_existing_created(self.__tablename__)

class Batch(QtCore.QObject, Finishable):
    finished = QtCore.pyqtSignal()
    progress = QtCore.pyqtSignal(int, int)

    def __init__(self, parent=None):
        QtCore.QObject.__init__(self)
        Finishable.__init__(self, self.finished)

        self.batch_proxy = BatchProxy()
        self.batch_proxy.moveToThread(dbmanager.worker)
        self.batch_proxy.commit.connect(self.handle_commit)
        dbmanager.register_batch(id(self), self.batch_proxy)

        self.children = set()
        self.finished_children = set()
        self.ops_started = 0
        self.ops_committed = 0
        self.finish_called = False

        self.parent = parent
        if parent is not None:
            parent.register_child(self)

    def add_op(self):
        if self.finish_called:
            raise Exception('op added to Batch after finish() called')
        self.ops_started = self.ops_started + 1

    def register_child(self, child):
        if self.finish_called:
            raise Exception('child added to Batch after finish() called')
        self.children.add(child)
        child.finished.connect(lambda: self.handle_child(child))
        self.check_for_finished()

    def handle_child(self, child):
        self.finished_children.add(child)
        self.check_for_finished()

    def handle_commit(self, ops):
        self.ops_committed = self.ops_committed + ops
        if self.ops_committed > self.ops_started:
            raise Exception('Batch has more ops started than committed')
        self.check_for_finished()

    def finish(self):
        self.finish_called = True
        self.check_for_finished()

    def check_for_finished(self):
        self.progress.emit(self.ops_committed + len(self.finished_children), self.ops_started + len(self.children))
        if self.finish_called and self.ops_started == self.ops_committed and len(self.children) == len(self.finished_children):
            self.finished.emit()

def conv_int(v):
    i,ok = v.toInt()
    return int(i)

def conv_str(v):
    return str(v.toString())

def conv_unicode(v):
    return unicode(v.toString())

def conv_float(v):
    i, ok = v.toDouble()
    return float(i)

class Event(DBBase):
    __tablename__ = 'event'
    __key__ = ['id']
    __fields__ = {'id': conv_int,
                  'name': conv_unicode,
                  }

class Photo(DBBase):
    __tablename__ = 'photo'
    __key__ = ['id']
    __fields__ = {'id': conv_int,
                  'url' : conv_unicode,
                  'filename' : conv_unicode,
                  'width': conv_int,
                  'height': conv_int,
                  'date_fetched' : conv_float,
                  'person_id' : conv_int,
                  'crop_centre_x' : conv_float,
                  'crop_centre_y' : conv_float,
                  'crop_scale' : conv_float,
                  'brightness' : conv_float,
                  'contrast' : conv_float,
                  'gamma' : conv_float,
                  'rotate' : conv_float,
                  'opinion' : conv_str,
                  }

    def url_filename(self):
        if self.url is not None:
            return self.url.split('/')[-1]

    def full_path(self):
        filename = self.url_filename()
        if not filename:
            filename = self.filename
        if filename:
            return os.path.join(photodir, filename)

    def update_crop(self, centre_x, centre_y, scale, origin=''):
        self.update({'id': self.id,
                     'crop_centre_x': centre_x,
                     'crop_centre_y': centre_y,
                     'crop_scale': scale,
                     }, origin)

    def update_opinion(self, opinion, origin=''):
        self.update({'id': self.id,
                     'opinion': opinion,
                     }, origin)

    def update_rotation(self, angle, origin=''):
        self.update({'id': self.id,
                     'rotate': angle,
                     }, origin)

    def update_brightness(self, brightness, origin=''):
        self.update({'id': self.id,
                     'brightness': brightness,
                     }, origin)

    def update_contrast(self, contrast, origin=''):
        self.update({'id': self.id,
                     'contrast': contrast,
                     }, origin)

    def update_gamma(self, gamma, origin=''):
        self.update({'id': self.id,
                     'gamma': gamma,
                     }, origin)

class Person(DBBase):
    __tablename__ = 'person'
    __key__ = ['id']
    __fields__ = {'id': conv_int,
                  'firstname': conv_unicode,
                  'lastname': conv_unicode,
                  'title': conv_unicode,
                  'fullname': conv_unicode,
                  'current_photo_id': conv_int,
                  'last_checked_at': conv_float,
                  'police_status': conv_unicode,
                  }

    def __repr__(self):
        return u"Person<%d: %s>" % (self.id, self.fullname)

    def __str__(self):
        return u"%d: %s" % (self.id, self.fullname)

    def update_current_photo(self, photo_id, origin=''):
        self.update({'id': self.id,
                     'current_photo_id': photo_id,
                     }, origin)

class Registration(DBBase):
    __tablename__ = 'registration'
    __key__ = ['person_id', 'event_id']
    __fields__ = {'person_id': conv_int,
                  'event_id': conv_int,
                  'attendee_type': conv_unicode,
                  'booking_ref': conv_unicode,
                  'booker_firstname': conv_unicode,
                  'booker_lastname': conv_unicode,
                  }

class QueryWorker(QtCore.QObject):
    finished = QtCore.pyqtSignal()
    exception = QtCore.pyqtSignal(Exception)

    def __init__(self, query_str, binds):
        QtCore.QObject.__init__(self)

        self.query_str = query_str
        self.binds = binds
        self.rows = None

    @QtCore.pyqtSlot()
    def run(self):
        try:
            query = QtSql.QSqlQuery()
            if not query.prepare(self.query_str):
                raise DBException("Prepare failed", query)
            for k, v in self.binds.iteritems():
                query.bindValue(':%s' % k, v)
            query.setForwardOnly(True)
            if not query.exec_():
                raise DBException("Query failed?", query)
            rows = []
            while query.next():
                i = 0
                row = []
                while True:
                    v = query.value(i)
                    if not v.isValid():
                        break
                    row.append(v)
                    i = i + 1
                rows.append(row)
            self.rows = rows
            self.finished.emit()
        except Exception, e:
            traceback.print_exc()
            self.exception.emit(e)

class Query(QtCore.QObject, Finishable):
    sig_run = QtCore.pyqtSignal()

    def __init__(self, query_str, binds={}):
        QtCore.QObject.__init__(self)

        self.worker = QueryWorker(query_str, binds)
        self.worker.moveToThread(dbmanager.worker)
        self.sig_run.connect(self.worker.run)
        self.finished = self.worker.finished
        self.exception = self.worker.exception

        Finishable.__init__(self, self.finished, self.exception)

    def run(self):
        self.sig_run.emit()

    def rows(self):
        return self.worker.rows

class FindPhotosWorker(QtCore.QObject):
    finished = QtCore.pyqtSignal()

    def __init__(self, which):
        QtCore.QObject.__init__(self)
        self.which = which
        self.ids = None

    @QtCore.pyqtSlot()
    def run(self):
        if self.which == 'missing':
            query = QtSql.QSqlQuery("""select person.id from person left outer join photo on person.current_photo_id = photo.id where person.current_photo_id is null or photo.opinion = 'bad'
                                       order by person.lastname, person.firstname, person.id""")
        elif self.which == 'good':
            query = QtSql.QSqlQuery("""select person.id from person join photo on person.current_photo_id = photo.id where photo.opinion = 'ok'
                                       order by person.lastname, person.firstname, person.id""")
        else:
            query = QtSql.QSqlQuery("""select person.id from person order by person.lastname, person.firstname, person.id""")
        ids = []
        while query.next():
            id, ok = query.value(0).toInt()
            ids.append(id)
        query.finish()
        self.ids = ids
        self.finished.emit()

    def result(self):
        return self.ids

class FindPhotos(Query):
    def __init__(self, which):
        if which == 'missing':
            query = """select person.id from person left outer join photo on person.current_photo_id = photo.id where person.current_photo_id is null or photo.opinion = 'bad'
                       order by person.lastname, person.firstname, person.id"""
        elif which == 'good':
            query = """select person.id from person join photo on person.current_photo_id = photo.id where photo.opinion = 'ok'
                       order by person.lastname, person.firstname, person.id"""
        else:
            query = """select person.id from person order by person.lastname, person.firstname, person.id"""
        Query.__init__(self, query)

    def result(self):
        def unpack_row(row):
            id, ok = row[0].toInt()
            return id
            
        return map(unpack_row, self.rows())

class FindRegistrations(Query):
    def __init__(self, person_id):
        self.person_id = person_id
        Query.__init__(self, 'select event_id from registration where person_id = :person_id', {'person_id': person_id})

    def result(self):
        def unpack_row(row):
            event_id, ok = row[0].toInt()
            return Registration(key={'person_id': self.person_id, 'event_id': event_id})
            
        return map(unpack_row, self.rows())

class FindCategories(Query):
    def __init__(self):
        Query.__init__(self, 'select distinct attendee_type from registration')

    def result(self):
        return map(lambda row: row[0].toString(), self.rows())        

class FindPoliceStatus(Query):
    def __init__(self):
        Query.__init__(self, 'select distinct police_status from person')

    def result(self):
        return map(lambda row: row[0].toString(), self.rows())        

class FetchPhotoHistory(QtCore.QObject):
    ready = QtCore.pyqtSignal(int)

    def __init__(self):
        QtCore.QObject.__init__(self)
        self.query = None
        self.photos = None
        self.people = LRUCache(size_limit=10)
        self.current_person = None
        self.next_person = None

    def run(self, person_id):
        if self.query is not None and not self.query.is_finished:
            self.next_person = person_id
            return

        self.current_person = person_id
        self.query = Query('select id from photo where person_id = :person_id', {'person_id': person_id})
        self.query.finished.connect(self.handle_query)
        self.query.run()

    def handle_query(self):
        self.photos = []
        signals = []
        for row in self.query.rows():
            photo_id, ok = row[0].toInt()
            photo = Photo(photo_id)
            self.photos.append(photo)
            signals.append(photo.updated)
        self.signalgroup = SignalGroup(*signals)
        self.signalgroup.fire.connect(self.handle_photos)

    def handle_photos(self):
        if self.current_person is None or self.photos is None:
            # Should never happen...
            return

        self.people[self.current_person] = self.photos
        self.ready.emit(self.current_person)

        self.current_person = None
        self.query = None
        if self.next_person is not None:
            self.run(self.next_person)
            self.next_person = None

    def get_photos(self, person_id):
        return self.people[person_id]

class FetchedPhotoWorker(QtCore.QObject):
    finished = QtCore.pyqtSignal()

    def __init__(self, person_id, url, batch, opinion=None, local_filename=None):
        QtCore.QObject.__init__(self)
        self.person_id = person_id
        self.url = url
        self.local_filename = local_filename
        self.opinion = opinion
        self.first_batch = Batch(batch)
        self.second_batch = Batch(batch)

    def find_photo(self):
        query = QtSql.QSqlQuery()
        if self.url is not None:
            query.prepare('select id from photo where person_id = :person_id and url = :url')
            query.bindValue(':url', self.url)
        else:
            query.prepare('select id from photo where person_id = :person_id and filename = :filename')
            query.bindValue(':filename', self.local_filename)
        query.bindValue(':person_id', self.person_id)
        query.setForwardOnly(True)
        query.exec_()
        if query.next():
            id, ok = query.value(0).toInt()
        else:
            id = None
        query.finish()
        return id

    @QtCore.pyqtSlot()
    def run(self):
        photo_id = self.find_photo()
        query = QtSql.QSqlQuery()
        query.setForwardOnly(True)

        new_values = {'date_fetched': time.time()}
        if self.opinion is not None:
            new_values['opinion'] = self.opinion
        
        if photo_id is not None:
            values = {'id': id}
            values.update(new_values)
            dbmanager.update('photo', values, 'FetchedPhoto', DBBase.batch_op(self.first_batch))
        else:
            values = {'person_id': self.person_id}
            if self.url is not None:
                values['url'] = self.url
            if self.local_filename is not None:
                values['filename'] = self.local_filename
            values.update(new_values)
            dbmanager.upsert('photo', values, 'FetchedPhoto', DBBase.batch_op(self.first_batch))

        self.first_batch.finished.connect(self.update_person)
        self.first_batch.finish()

    def update_person(self):
        photo_id = self.find_photo()
        dbmanager.update('person', {'id': self.person_id, 'current_photo_id': photo_id}, 'FetchedPhoto', DBBase.batch_op(self.second_batch))
        self.second_batch.finish()

class FetchedPhoto(QtCore.QObject):
    sig_run = QtCore.pyqtSignal()

    def __init__(self, *args, **kwargs):
        QtCore.QObject.__init__(self)

        self.worker = FetchedPhotoWorker(*args, **kwargs)
        self.worker.moveToThread(dbmanager.worker)
        self.sig_run.connect(self.worker.run)

    def run(self):
        self.sig_run.emit()

def stash_photo(filename):
    local_filename = os.path.basename(filename)
    shutil.copy(filename, os.path.join(photodir, local_filename))
    return local_filename

def setup_session(datadir):
    global engine, photodir
    photodir = os.path.join(datadir, 'photos')
    if not os.path.exists(photodir):
        os.mkdir(photodir)
    dbfile = os.path.join(datadir, 'people.sqlite')
    db = QtSql.QSqlDatabase.addDatabase('QSQLITE')
    db.setDatabaseName(dbfile)
    if not db.open():
        raise DBException("Failed to open db", db)

    query = QtSql.QSqlQuery()
    person_record = db.record('person')
    if person_record.isEmpty():
        query.exec_("""CREATE TABLE person (
                       id INTEGER NOT NULL,
                       firstname VARCHAR,
                       lastname VARCHAR,
                       title VARCHAR,
                       fullname VARCHAR,
                       police_status varchar,
                       current_photo_id INTEGER,
                       last_checked_at DATETIME,
                       PRIMARY KEY (id)
                       )""")
    else:
        if not person_record.contains('police_status'):
            query.exec_("""alter table person add column police_status varchar""")
    photo_record = db.record('photo')
    if photo_record.isEmpty():
        query.exec_('''CREATE TABLE photo (
                       id INTEGER NOT NULL,
                       url VARCHAR,
                       filename varchar,
                       date_fetched DATETIME,
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
                       opinion VARCHAR(6) default 'unsure',
                       PRIMARY KEY (id),
                       UNIQUE(url, person_id),
                       FOREIGN KEY(person_id) REFERENCES person (id),
                       CONSTRAINT photo_opinion CHECK (opinion IN ('ok', 'bad', 'unsure'))
                       )''')
    else:
        if not photo_record.contains('filename'):
            query.exec_('''alter table photo add column filename varchar''')
        if not photo_record.contains('width'):
            query.exec_('''alter table photo add column width integer default 0''')
        if not photo_record.contains('height'):
            query.exec_('''alter table photo add column height integer default 0''')
        if not photo_record.contains('brightness'):
            query.exec_('''alter table photo add column brightness float default 0.0''')
        if not photo_record.contains('contrast'):
            query.exec_('''alter table photo add column contrast float default 0.0''')
        if not photo_record.contains('gamma'):
            query.exec_('''alter table photo add column gamma float default 1.0''')
            
    if db.record('event').isEmpty():
        query.exec_('''CREATE TABLE event (
	               id INTEGER NOT NULL,
                       name VARCHAR,
                       PRIMARY KEY (id)
                       )''')
    if db.record('registration').isEmpty():
        query.exec_('''CREATE TABLE registration (
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

    dbmanager.start()
