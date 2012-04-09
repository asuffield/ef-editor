import os
import weakref
import time
from PyQt4 import QtCore, QtSql
from ef.threads import WorkerThread
from collections import deque

photodir = None

# We can't pass a signal directly into the db worker thread, because
# there's no apparent way to put pyqtBoundSignal into a type
# signature. So, we pass these proxy objects around and chain
# connections through them
class DBBaseProxy(QtCore.QObject):
    # Should probably make the origin a set instead of a string...
    update = QtCore.pyqtSignal(dict, str)

class DBException(Exception):
    def __init__(self, msg, query):
        self.msg = msg
        self.error = query.lastError().text()
    def __str__(self):
        return "%s: %s" % (self.msg, self.error)

class DBWorker(QtCore.QObject):
    created = QtCore.pyqtSignal(str, dict, str)
    updated = QtCore.pyqtSignal(str, dict, str)

    # In order to improve application performance and decrease the
    # number of spurious database operations, we batch updates
    # together before shipping them off to the database. An update
    # goes into self.update_queue initially, and any further updates
    # get applied to the object stored there. When the timer ticks, we
    # write out the queue to the database.

    # Note that we don't do this for upsert or insert, only
    # update. This is because updates happen at high frequency due to
    # UI operations, while the other operations are only used on
    # fetching from eventsforce
    
    def __init__(self):
        QtCore.QObject.__init__(self)

        self.binds = {}
        self.update_queue = {}

    @QtCore.pyqtSlot()
    def setup(self):
        self.timer = QtCore.QTimer(self)
        self.timer.setInterval(300)
        self.timer.timeout.connect(self.timeout)
        self.timer.start()

    def timeout(self):
        for table in self.update_queue:
            for key in self.update_queue[table]:
                queued = self.update_queue[table][key]
                self.do_update(table, queued['key_fields'], queued['values'], queued['origin'])
        self.update_queue = {}
        
        # Remove all invalidated weak references
        for x in self.binds.itervalues():
            for bindings in x.itervalues():
                bindings = filter(lambda binding: binding['proxy']() is not None, bindings)

    def get_binds(self, table, key):
        k = tuple(sorted(key.items()))
        return self.binds.setdefault(table, {}).setdefault(k, [])

    def get_queued_update(self, table, key):
        k = tuple(sorted(key.items()))
        return self.update_queue.setdefault(table, {}).setdefault(k, {'values': {}, 'origin': set()})

    @QtCore.pyqtSlot(str, list, dict, DBBaseProxy)
    def bind(self, table, fields, key, proxy):
        binding = {'fields': fields, 'proxy': weakref.ref(proxy)}
        self.get_binds(table, key).append(binding)
        self.fetch(table, fields, key, binding)

    def key_expr(self, key):
        return ' and '.join(map(lambda k: '%s = :%s' % (k,k), key))

    def fetch(self, table, fields, key, binding):
        if len(key) == 0 or len(fields) == 0:
            raise TypeError("No key/fields supplied")
        
        query = QtSql.QSqlQuery()
        if not query.prepare('select %s from %s where %s' % (','.join(fields), table, self.key_expr(key))):
            raise DBException("Prepare failed", query)
        for k, v in key.iteritems():
            query.bindValue(':%s' % k, v)
        query.setForwardOnly(True)
        if not query.exec_():
            raise DBException("Query failed?", query)

        if query.next():
            results = {}
            for i, k in enumerate(fields):
                results[k] = query.value(i)
            proxy = binding['proxy']()
            if proxy is not None:
                proxy.update.emit(results, 'bind')

        query.finish()

    # This should probably just be something that happens automatically when the thread starts...
    @QtCore.pyqtSlot(str, list)
    def signal_existing_created(self, table, key_fields):
        query = QtSql.QSqlQuery()
        if not query.prepare('select %s from %s' % (','.join(key_fields), table)):
            raise DBException('Prepare failed', query)
        query.exec_()
        while query.next():
            values = map(lambda i: query.value(i), xrange(0, len(key_fields)))
            key = dict(zip(key_fields, values))
            self.created.emit(table, key, 'signal_existing')
        query.finish()

    @QtCore.pyqtSlot(str, list, dict, str)
    def update(self, table, key_fields, values, origin):
        key = dict([(k,values[k]) for k in key_fields])
        queued = self.get_queued_update(table, key)
        queued['key_fields'] = key_fields
        queued['values'].update(values)
        queued['origin'].add(origin)
    
    def do_update(self, table, key_fields, values, origin):
        # Note that QtSql won't let us use the same name twice, so we can't include key_fields in value_fields
        value_fields = set(values) - set(key_fields)
        
        query = QtSql.QSqlQuery()
        query.prepare('update %s set %s where %s' % (table, ','.join(map(lambda k: "%s = :%s" % (k,k), value_fields)), self.key_expr(key_fields)))
        for k, v in values.iteritems():
            query.bindValue(':%s' % k, v)
        query.setForwardOnly(True)
        if not query.exec_():
            raise DBException("Query failed!?", query)
        query.finish()

        key = dict([(k,values[k]) for k in key_fields])

        for binding in self.get_binds(table, key):
            proxy = binding['proxy']()
            if proxy is not None:
                for o in origin:
                    proxy.update.emit(values, o)

        for o in origin:
            self.updated.emit(table, key, o)

    @QtCore.pyqtSlot(str, list, dict, str)
    def upsert(self, table, key_fields, values, origin):
        db = QtSql.QSqlDatabase.database()
        db.transaction()
        try:
            query = QtSql.QSqlQuery()
            stmt = 'select %s from %s where %s' % (','.join(key_fields), table, self.key_expr(key_fields))
            if not query.prepare(stmt):
                raise DBException("Prepare failed!", query)
            for k in key_fields:
                query.bindValue(':%s' % k, values[k])
            query.setForwardOnly(True)
            if not query.exec_():
                raise DBException("Query failed!", query)
            if query.next():
                self.do_update(table, key_fields, values, [origin])
            else:
                self.insert(table, key_fields, values, origin)
            query.finish()
            db.commit()
        except:
            db.rollback()
            raise

    @QtCore.pyqtSlot(str, dict, str)
    def insert(self, table, key_fields, values, str):
        query = QtSql.QSqlQuery()
        keys = values.keys()
        query.prepare('insert into %s (%s) values (%s) returning id' % (table, ','.join(keys), ','.join(map(lambda k: ':' + k), keys)))
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
        self.created.emit(table, key, origin)

    # This is a hideous abstraction violation, but it's better than
    # building an elaborate infrastructure just to support this one
    # transaction

    # If we get more of these, rethink how all this works. Problem is
    # that we need logic inside the transaction, hence we need logic
    # in the db thread... and if the logic has to be shoved somewhere,
    # why not here?
    @QtCore.pyqtSlot(int, str)
    def fetched_photo(self, person_id, url):
        db = QtSql.QSqlDatabase.database()
        db.transaction()
        try:
            id = self.find_photo(person_id, url)
            
            if id is not None:
                self.do_update('photo', ['id'], {'id': id,
                                                 'date_fetched': time.time(),
                                                 }, ['fetched_photo'])
            else:
                self.insert('photo', ['id'], {'url' : url,
                                              'date_fetched' : time.time(),
                                              'person_id' : person_id,
                                              'crop_centre_x' : 0.5,
                                              'crop_centre_y' : 0.5,
                                              'crop_scale' : 1,
                                              'rotate' : 0,
                                              'opinion' : 'unsure',
                                              }, 'fetched_photo')
                id = self.find_photo(person_id, url)

            self.do_update('person', ['id'], {'id': person_id,
                                              'current_photo_id': id,
                                              }, ['fetched_photo'])
            db.commit()
        except:
            db.rollback()
            raise

    def find_photo(self, person_id, url):
        query = QtSql.QSqlQuery()
        query.prepare('select id from photo where person_id = :person_id and url = :url')
        query.bindValue(':person_id', person_id)
        query.bindValue(':url', url)
        query.setForwardOnly(True)
        query.exec_()
        if query.next():
            id = query.value(0)
        else:
            id = None
        query.finish()
        return id

class DBManager(QtCore.QObject):
    sig_bind = QtCore.pyqtSignal(str, list, dict, DBBaseProxy)
    sig_update = QtCore.pyqtSignal(str, list, dict, str)
    sig_upsert = QtCore.pyqtSignal(str, list, dict, str)
    sig_insert = QtCore.pyqtSignal(str, list, dict, str)
    sig_fetched_photo = QtCore.pyqtSignal(int, str)
    sig_signal_existing_created = QtCore.pyqtSignal(str, list)

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
        self.sig_insert.connect(self.dbworker.insert)
        self.sig_fetched_photo.connect(self.dbworker.fetched_photo)
        self.sig_signal_existing_created.connect(self.dbworker.signal_existing_created)
        self.dbworker.updated.connect(self.updated)
        self.dbworker.created.connect(self.created)
        self.worker.started.connect(self.dbworker.setup)

    def start(self):
        self.worker.start()

    # Wrappers to hide the Qt noise

    def bind(self, table, fields, key, signal):
        self.sig_bind.emit(table, fields, key, signal)

    def update(self, table, key_fields, values, origin):
        self.sig_update.emit(table, key_fields, values, origin)

    def upsert(self, table, key_fields, values, origin):
        self.sig_upsert.emit(table, key_fields, values, origin)

    def insert(self, table, key_fields, values, origin):
        self.sig_insert.emit(table, key_fields, values, origin)

    def signal_existing_created(self, table, key_fields):
        self.sig_signal_existing_created.emit(table, key_fields)

    def fetched_photo(self, person_id, url):
        self.sig_fetched_photo.emit(person_id, url)

dbmanager = DBManager()

class DBBase(QtCore.QObject):
    updated = QtCore.pyqtSignal(str)

    def __init__(self, *key_fields):
        QtCore.QObject.__init__(self)
        for k in self.__fields__:
            setattr(self, k, None)
        key = {}
        for k, v in zip(self.__key__, key_fields):
            setattr(self, k, v)
            key[k] = v

        self.__proxy = DBBaseProxy()
        self.__proxy.update.connect(self._do_update)
        dbmanager.bind(self.__tablename__, self.__fields__.keys(), key, self.__proxy)

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
    def insert(self, values):
        self._check_values(values)
        dbmanager.insert(self.__tablename__, self.__key__, values)

    def update(self, values, origin=''):
        self._check_values(values)
        dbmanager.update(self.__tablename__, self.__key__, values, origin)

    @classmethod
    def upsert(self, values, origin=''):
        self._check_values(values)
        dbmanager.upsert(self.__tablename__, self.__key__, values, origin)

    @classmethod
    def signal_existing_created(self):
        dbmanager.signal_existing_created(self.__tablename__, self.__key__)

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
    __fields__ = {'id': int,
                  'name': str,
                  }

class Photo(DBBase):
    __tablename__ = 'photo'
    __key__ = ['id']
    __fields__ = {'id': conv_int,
                  'url' : conv_unicode,
                  'date_fetched' : conv_float,
                  'person_id' : conv_int,
                  'crop_centre_x' : conv_float,
                  'crop_centre_y' : conv_float,
                  'crop_scale' : conv_float,
                  'rotate' : conv_float,
                  'opinion' : conv_str,
                  }

    def url_filename(self):
        if self.url is not None:
            return self.url.split('/')[-1]

    def full_path(self):
        filename = self.url_filename()
        if filename is not None:
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

    @classmethod
    def fetched(self, person_id, url):
        dbmanager.fetched_photo(person_id, url)

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
                  }
    
    def __repr__(self):
        return u"Person<%d: %s>" % (self.id, self.fullname)
    
    def __str__(self):
        return u"%d: %s" % (self.id, self.fullname)

class Registration(DBBase):
    __tablename__ = 'registration'
    __key__ = ['person_id', 'event_id']
    __fields__ = {'person_id': conv_int,
                  'event_id': conv_int,
                  'attendee_type': conv_unicode,
                  'booking_ref': conv_unicode,
                  'booker_email': conv_unicode,
                  'booker_firstname': conv_unicode,
                  'booker_lastname': conv_unicode,
                  }

class FindUnsureQuerier(QtCore.QObject):
    query_result = QtCore.pyqtSignal(int)

    def __init__(self):
        super(QtCore.QObject, self).__init__()
        self.buffer = deque()
        self.visited = set()

    def run_sql_query(self):
        query = QtSql.QSqlQuery("""select person.id from photo join person on photo.id = person.current_photo_id where photo.opinion = 'unsure' order by person.lastname, person.firstname, person.id""")
        while query.next():
            id, ok = query.value(0).toInt()
            if id in self.visited:
                continue
            self.buffer.append(id)
            self.visited.add(id)
        query.finish()

    @QtCore.pyqtSlot(int)
    def query_one(self):
        if len(self.buffer) == 0:
            self.run_sql_query()
        if len(self.buffer) == 0:
            # Since we found nothing this time, we'll notify failure
            # and reset the visited set, so future queries will redo
            # from the start
            self.visited = set()
            self.query_result.emit(-1)
            return

        id = self.buffer.popleft()
        self.query_result.emit(id)

class FindUnsure(QtCore.QObject):
    query_one = QtCore.pyqtSignal()

    def __init__(self):
        QtCore.QObject.__init__(self)
        self.callbacks = deque()
        
        self.querier = FindUnsureQuerier()
        self.querier.moveToThread(dbmanager.worker)
        self.querier.query_result.connect(self._handle_result)
        self.query_one.connect(self.querier.query_one)

    def next(self, callback):
        self.callbacks.append(callback)
        self.query_one.emit()

    def _handle_result(self, id):
        callback = self.callbacks.popleft()
        if id < 0:
            callback(None)
        else:
            callback(int(id))

class FindPhotosWorker(QtCore.QObject):
    results = QtCore.pyqtSignal(list)

    def __init__(self, which):
        QtCore.QObject.__init__(self)
        self.which = which

    @QtCore.pyqtSlot()
    def run(self):
        if self.which == 'missing':
            query = QtSql.QSqlQuery("""select person.id from person left outer join photo on person.current_photo_id = photo.id where person.current_photo_id is null or photo.opinion = 'bad'""")
        else:
            query = QtSql.QSqlQuery("""select person.id from person""")
        ids = []
        while query.next():
            id, ok = query.value(0).toInt()
            ids.append(id)
        query.finish()
        self.results.emit(ids)

class FindPhotos(QtCore.QObject):
    sig_run = QtCore.pyqtSignal()
    results = QtCore.pyqtSignal(list)
    
    def __init__(self, which):
        QtCore.QObject.__init__(self)

        self.worker = FindPhotosWorker(which)
        self.worker.moveToThread(dbmanager.worker)
        self.sig_run.connect(self.worker.run)
        self.worker.results.connect(self.results)

    def run(self):
        self.sig_run.emit()

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
    if db.record('person').isEmpty():
        query.exec_('''CREATE TABLE person (
                       id INTEGER NOT NULL, 
                       firstname VARCHAR, 
                       lastname VARCHAR, 
                       title VARCHAR, 
                       fullname VARCHAR, 
                       current_photo_id INTEGER, 
                       last_checked_at DATETIME, 
                       PRIMARY KEY (id)
                       )''')
    if db.record('photo').isEmpty():
        query.exec_('''CREATE TABLE photo (
                       id INTEGER NOT NULL, 
                       url VARCHAR, 
                       date_fetched DATETIME, 
                       person_id INTEGER, 
                       crop_centre_x FLOAT, 
                       crop_centre_y FLOAT, 
                       crop_scale FLOAT, 
                       rotate FLOAT, 
                       opinion VARCHAR(6), 
                       PRIMARY KEY (id), 
                       FOREIGN KEY(person_id) REFERENCES person (id), 
                       CONSTRAINT photo_opinion CHECK (opinion IN ('ok', 'bad', 'unsure'))
                       )''')
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
