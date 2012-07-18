import os
import weakref
import time
import traceback
import shutil
import sys
import multiprocessing
import Queue
import errno
from PyQt4 import QtCore
from ef.task import Finishable
from ef.lib import LRUCache, SignalGroup
from ef.dbworker import start_dbworker
from multiprocessing.queues import Queue as MPQueue

photodir = None

class DBDataManager(object):
    def __init__(self):
        self.classes = {}
        self.objects = {}
        self.dbmanager = None

    def object_key(self, table, key):
        return (table, tuple(sorted(key.items())))

    def get(self, cls, key):
        table = cls.__tablename__
        return self.objects[self.object_key(table, key)]

    def create(self, table, key, values):
        rec = self.classes[table]
        obj = rec['class'](key, values)
        k = self.object_key(table, obj.__key__)
        if self.objects.has_key(k):
            raise Exception("Duplicate object arrived from db worker!", k)
        self.objects[k] = obj
        self.objects.setdefault(table, []).append(obj)
        return obj

    def all(self, cls):
        return self.objects.get(cls.__tablename__, [])

    def update(self, table, key, values, origin, suppress_updates=False):
        rec = self.classes[table]
        k = self.object_key(table, key)
        obj = self.objects[k]
        obj._do_update(values, origin, suppress_updates)
        return obj

    def register_class(self, dbclass, name):
        rec = {'name': name,
               'table': dbclass.__tablename__,
               'key': dbclass.__key__,
               'fields': dbclass.__fields__,
               'class': dbclass,
               }
        if self.classes.has_key(rec['table']):
            raise Exception("Multiple classes defined for table %s (other is %s)" % (rec['table'], self.classes[rec['table']]['name']))
        self.classes[rec['table']] = rec

dbdata = DBDataManager()

class DBBaseMeta(type(QtCore.QObject)):
    def __init__(self, name, bases, dict):
        super(DBBaseMeta, self).__init__(name, bases, dict)
        if not hasattr(self, '__tablename__'):
            return

        dbdata.register_class(self, name)

class DBBase(QtCore.QObject):
    __metaclass__ = DBBaseMeta
    updated = QtCore.pyqtSignal(set)

    def __init__(self, key, values):
        QtCore.QObject.__init__(self)

        values.update(key)

        object.__setattr__(self, '__key__', key)
        object.__setattr__(self, '__values__', values)

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

    def _do_update(self, data, origin, suppress_updates):
        for k,v in data.iteritems():
            try:
                if isinstance(v, QtCore.QVariant):
                    v = self.__fields__[k](v)
                self.__values__[k] = v
            except TypeError:
                self.__values__[k] = None
        if not suppress_updates:
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
        values = dict(values)
        values.update(kwargs)
        values.update(self.__key__)
        self._check_values(values)
        dbdata.dbmanager.update(self.__tablename__, values, origin, self.batch_op(batch))

    @classmethod
    def upsert(self, values={}, origin='', batch=None, **kwargs):
        values = dict(values)
        values.update(kwargs)
        dbdata.dbmanager.upsert(self.__tablename__, values, origin, self.batch_op(batch))

    @classmethod
    def signal_existing_created(self):
        dbdata.dbmanager.signal_existing_created(self.__tablename__)

    @classmethod
    def get(self, **kwargs):
        self._check_values(kwargs)
        return dbdata.get(self, kwargs)

    @classmethod
    def all(self):
        return dbdata.all(self)

def retry_on_eintr(f, *args, **kw):
    while True:
        try:
            return f(*args, **kw)
        except IOError, e:            
            if e.errno == errno.EINTR:
                continue
            else:
                raise    

class RetryQueue(MPQueue):
    """Queue which will retry if interrupted with EINTR."""
    def get(self, block=True, timeout=None):
        return retry_on_eintr(MPQueue.get, self, block, timeout)

class DBManager(QtCore.QObject):
    created = QtCore.pyqtSignal(DBBase, set)
    exception = QtCore.pyqtSignal(Exception, str)
    existing_done = QtCore.pyqtSignal(str)
    process_done = QtCore.pyqtSignal(str, str)

    def __init__(self, datadir):
        super(QtCore.QObject, self).__init__()

        self.write_queue = RetryQueue()
        self.result_queue = RetryQueue()
        self.process = multiprocessing.Process(None, start_dbworker, 'dbworker', (self.write_queue, self.result_queue, str(datadir)))
        self.process.start()

        self.timer = QtCore.QTimer(self)
        self.timer.setInterval(100)
        self.timer.timeout.connect(self.poll)
        self.timer.start()

        self.pending_op_count = 0
        self.batches = weakref.WeakValueDictionary()
        self.is_shutdown = False

        self.is_importing = False
        self.import_queue = []
        self.import_updates = []

    def shutdown(self):
        if self.is_shutdown:
            return

        self.timer.stop()
        self.is_shutdown = True
        self.write_queue.put('STOP')
        while True:
            op, result = self.result_queue.get()
            if op == 'shutdown':
                self.process.join()
                return

    def register_batch(self, id, batch):
        self.batches[id] = batch

    def post(self, op, args):
        #print 'manager posting', op, args
        self.write_queue.put((op, args))

    def poll(self):
        if self.is_shutdown:
            return

        if not self.process.is_alive():
            self.is_shutdown = True
            self.timer.stop()
            self.exception.emit(Exception('Database worker process died'), 'Database worker process died')
            return
        
        try:
            while True:
                op, result = self.result_queue.get(False)
                #print 'manager result', op, result
                if op == 'exception':
                    print 'exception', result
                    self.exception.emit(result[0], result[1])
                elif op == 'batch_committed':
                    id, count = result
                    batch = self.batches.get(id, None)
                    if batch is not None:
                        batch.committed(count)
                elif op == 'pending':
                    self.pending_op_count = result
                elif op == 'fetch' or op == 'insert':
                    obj = dbdata.create(result['table'], result['key'], result['values'])
                    if self.is_importing:
                        self.import_queue.append(obj)
                    else:
                        self.created.emit(obj, result.get('origin', set(['fetch'])))
                elif op == 'fetch_all':
                    self.existing_done.emit(result)
                elif op == 'update':
                    obj = dbdata.update(result['table'], result['key'], result['values'], result['origin'], suppress_updates=self.is_importing)
                    if self.is_importing:
                        self.import_updates.append((obj, result['origin']))
                elif op == 'import':
                    for obj in self.import_queue:
                        self.created.emit(obj, set(['import']))
                    for obj, origin in self.import_updates:
                        obj.updated.emit(origin)
                    self.import_queue = []
                    self.import_updates = {}
                    self.is_importing = False
                    self.process_done.emit(op, result)
                elif op == 'export':
                    self.process_done.emit(op, result)
                else:
                    print 'Unexpected op from dbworker', op
        except Queue.Empty:
            pass
        except Exception, e:
            self.exception.emit(e, traceback.format_exc())

    def pending(self):
        return self.pending_op_count

    def update(self, table, values, origin, batchid):
        self.post('update', (table, values, origin, batchid))

    def upsert(self, table, values, origin, batchid):
        self.post('upsert', (table, values, origin, batchid))

    def signal_existing_created(self, table):
        self.post('fetch_all', table)

    def export_data(self, filename):
        self.post('export', filename)

    def import_data(self, filename):
        self.is_importing = True
        self.post('import', filename)

class Batch(QtCore.QObject, Finishable):
    finished = QtCore.pyqtSignal()
    progress = QtCore.pyqtSignal(int, int)

    def __init__(self, parent=None):
        QtCore.QObject.__init__(self)
        Finishable.__init__(self, self.finished)

        dbdata.dbmanager.register_batch(id(self), self)

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

    def committed(self, ops):
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

def conv_bool(v):
    return v.toBool()

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
                  'block_upload' : conv_bool,
                  }

    by_person_dict = {}

    def __init__(self, *args, **kwargs):
        DBBase.__init__(self, *args, **kwargs)

        Photo.by_person(self.person_id).append(self)

    @classmethod
    def by_person(self, person_id):
        return Photo.by_person_dict.setdefault(person_id, [])

    def url_filename(self):
        if self.url is not None:
            return self.url.split('/')[-1]
        return None

    def local_filename(self):
        filename = self.url_filename()
        if not filename:
            filename = self.filename
        return filename

    def full_path(self):
        filename = self.local_filename()
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

    def update_block_upload(self, state, origin=''):
        self.update({'id': self.id,
                     'block_upload': state,
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

    statuses = set()

    def __init__(self, *args, **kwargs):
        DBBase.__init__(self, *args, **kwargs)

        Person.statuses.add(self.police_status)

    def __repr__(self):
        return u"Person<%d: %s>" % (self.id, self.fullname)

    def __str__(self):
        return u"%d: %s" % (self.id, self.fullname)

    def update_current_photo(self, photo_id, origin=''):
        self.update({'id': self.id,
                     'current_photo_id': photo_id,
                     }, origin)

    @classmethod
    def all_with_photos(self, which):
        if which == 'missing':
            f = lambda person: person.current_photo_id is None or Photo.get(id=person.current_photo_id).opinion == 'bad'
        elif which == 'good':
            f = lambda person: person.current_photo_id is not None and Photo.get(id=person.current_photo_id).opinion == 'ok'
        else:
            f = lambda person: True

        return sorted(filter(f, Person.all()), key=lambda person: (person.lastname, person.firstname, person.id))

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

    categories = set()
    by_person_dict = {}

    def __init__(self, *args, **kwargs):
        DBBase.__init__(self, *args, **kwargs)

        Registration.categories.add(self.attendee_type)
        Registration.by_person(self.person_id).append(self)

    @classmethod
    def by_person(self, person_id):
        return Registration.by_person_dict.setdefault(person_id, [])

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
            #print 'prepare query'
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

class FetchedPhoto(QtCore.QObject):
    def __init__(self, person, url, batch, opinion=None, local_filename=None):
        QtCore.QObject.__init__(self)

        self.person = person
        self.url = url
        self.local_filename = local_filename
        self.opinion = opinion
        self.first_batch = Batch(batch)
        self.second_batch = Batch(batch)

        photo = self.find_photo()

        if photo is None:
            values = {'date_fetched': time.time()}
            values['person_id'] = self.person.id
            if self.opinion is not None:
                values['opinion'] = self.opinion
            if self.url is not None:
                values['url'] = self.url
            if self.local_filename is not None:
                values['filename'] = self.local_filename
            Photo.upsert(values=values, origin='FetchedPhoto', batch=self.first_batch)

        self.first_batch.finished.connect(self.update_person)
        self.first_batch.finish()

    def update_person(self):
        photo = self.find_photo()
        self.person.update(current_photo_id=photo.id, origin='FetchedPhoto', batch=self.second_batch)
        self.second_batch.finish()

    def find_photo(self):
        for photo in Photo.by_person(self.person.id):
            if self.url is not None:
                if photo.url == self.url:
                    return photo
            else:
                if photo.filename == self.local_filename:
                    return photo
        return None

def stash_photo(filename):
    local_filename = os.path.basename(filename)
    shutil.copy(filename, os.path.join(photodir, local_filename))
    return local_filename

def setup_session(datadir):
    global photodir
    photodir = os.path.join(datadir, 'photos')
    if not os.path.exists(photodir):
        os.mkdir(photodir)

    dbmanager = DBManager(datadir)
    dbdata.dbmanager = dbmanager
    return dbmanager
