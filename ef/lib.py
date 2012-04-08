from PyQt4 import QtCore, QtGui, QtNetwork
from collections import OrderedDict, deque
from ef.db import Session, Person, Photo
from ef.netlib import qt_page_get
import os

max_editing_width = 1200
max_editing_height = 1600

class PhotoLoader(QtCore.QObject):
    photo_ready = QtCore.pyqtSignal(int, QtGui.QImage)
    photo_fail = QtCore.pyqtSignal(int, str)

    def __init__(self):
        QtCore.QObject.__init__(self)
        self.manager = None
        self.handlers = {}

    @QtCore.pyqtSlot(int, str, QtCore.QSize)
    def load_image(self, id, filename, url, scale_size):
        if os.path.exists(filename):
            self.read_image(id, filename, scale_size)
            return

        if self.manager is None:
            self.manager = QtNetwork.QNetworkAccessManager()

        reply = qt_page_get(self.manager, url)
        
        # The purpose of this hash is to stop the lambda from being
        # garbage-collected, because python can't see references from
        # Qt. Otherwise the callback will crash.
        self.handlers[id] = lambda: self.image_fetched(id, filename, scale_size, reply)
        reply.finished.connect(self.handlers[id])

    def image_fetched(self, id, filename, scale_size, reply):
        reply.finished.disconnect(self.handlers[id])
        f = open(filename, 'w')
        f.write(reply.readAll())
        f.close()
        self.read_image(id, filename, scale_size)

    def read_image(self, id, filename, scale_size):
        reader = QtGui.QImageReader(filename)
        image = reader.read()

        if image.isNull():
            self.photo_fail.emit(id, reader.errorString())
        else:
            if scale_size.isValid():
                image = image.scaled(scale_size, QtCore.Qt.KeepAspectRatio)
            else:
                # Get outsized images down to working dimensions, so that we aren't hauling around megabytes of excess pixels
                width = image.width()
                height = image.height()
                want_scale = False
                while width > max_editing_width or height > max_editing_height:
                    width = width / 2
                    height = height / 2
                    want_scale = True
                if want_scale:
                    image = image.scaled(width, height, QtCore.Qt.KeepAspectRatio)
            self.photo_ready.emit(id, image)

class ThreadRegistry(QtCore.QObject):
    def __init__(self):
        super(QtCore.QObject, self).__init__()
        self.threads = []

    def add(self, thread):
        self.threads.append(thread)

    def wait_all(self):
        for thread in self.threads:
            thread.wait()
        self.threads = []

    def shutdown(self, rc):
        for thread in self.threads:
            thread.exit(rc)

thread_registry = ThreadRegistry()

class WorkerThread(QtCore.QThread):
    def __init__(self, *args, **kwargs):
        super(QtCore.QThread, self).__init__(*args, **kwargs)
        thread_registry.add(self)

class LimitedSizeDict(OrderedDict):
  def __init__(self, *args, **kwds):
      self.size_limit = kwds.pop("size_limit", None)
      OrderedDict.__init__(self, *args, **kwds)
      self._check_size_limit()

  def __setitem__(self, key, value):
      if key in self:
          del self[key]
      OrderedDict.__setitem__(self, key, value)
      self._check_size_limit()

  def _check_size_limit(self):
      if self.size_limit is not None:
          while len(self) > self.size_limit:
              self.popitem(last=False)

class PhotoCache(QtCore.QObject):
    start_loading = QtCore.pyqtSignal(int, str, str, QtCore.QSize)
    
    def __init__(self, limit):
        super(QtCore.QObject, self).__init__()
        self.cache = LimitedSizeDict(size_limit=limit)
        self.loading = set()
        self.handlers = {}
        self.worker = WorkerThread()
        self.loader = PhotoLoader()
        self.loader.moveToThread(self.worker)
        self.loader.photo_ready.connect(self._photo_ready)
        self.loader.photo_fail.connect(self._photo_fail)
        self.start_loading.connect(self.loader.load_image)

        self.worker.start()
        
    def load_image(self, id, filename, url, ready_cb=None, fail_cb=None, scale_size=QtCore.QSize()):
        if id in self.loading:
            return

        if self.cache.has_key(id):
            self._bump_cache_entry(id)
            if ready_cb is not None:
                ready_cb(id, self.cache[id])
            return
            
        self.handlers.setdefault(id, []).append({'ready': ready_cb, 'fail': fail_cb})
        self.loading.add(id)
        self.start_loading.emit(id, filename, url, scale_size)

    def _bump_cache_entry(self, id):
        # This bumps an item up to the head of the cache
        if self.cache.has_key(id):
            self.cache[id] = self.cache.pop(id)

    def peek_image(self, id):
        self._bump_cache_entry(id)
        return self.cache.get(id, None)

    def _cleanup_after_load(self, id):
        self.handlers.pop(id, None)
        self.loading.discard(id)

    def _photo_ready(self, id, image):
        pixmap = QtGui.QPixmap.fromImage(image)
        self.cache[id] = pixmap
        for handler in self.handlers.get(id, []):
            if handler['ready'] is not None:
                handler['ready'](id, pixmap)
        self._cleanup_after_load(id)

    def _photo_fail(self, id, error):
        for handler in self.handlers.get(id, []):
            if handler['fail'] is not None:
                handler['fail'](id, error)
        self._cleanup_after_load(id)

class FindUnsureQuerier(QtCore.QObject):
    query_result = QtCore.pyqtSignal(int)

    def __init__(self):
        super(QtCore.QObject, self).__init__()
        self.buffer = deque()
        self.visited = set()

    def run_sql_query(self):
        for p, photo in Session.query(Person, Photo).join(Person.current_photo).filter(Photo.opinion=='unsure').order_by(Person.lastname).order_by(Person.firstname).order_by(Person.id).all():
            if photo.id in self.visited:
                continue
            self.buffer.append(photo.id)
            self.visited.add(photo.id)

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
        super(QtCore.QObject, self).__init__()
        self.callbacks = deque()
        
        self.worker = WorkerThread()
        self.querier = FindUnsureQuerier()
        self.querier.moveToThread(self.worker)
        self.querier.query_result.connect(self._handle_result)
        self.query_one.connect(self.querier.query_one)
        
        self.worker.start()

    def next(self, callback):
        self.callbacks.append(callback)
        self.query_one.emit()

    def _handle_result(self, id):
        callback = self.callbacks.popleft()
        if id < 0:
            callback(None)
        else:
            callback(Photo.by_id(id))

class DBUpdateWorker(QtCore.QObject):
    def __init__(self):
        super(QtCore.QObject, self).__init__()

    @QtCore.pyqtSlot(int, float, float, float)
    def update_photo_crop(self, id, centre_x, centre_y, scale):
        photo = Photo.by_id(id)
        photo.crop_centre_x = centre_x
        photo.crop_centre_y = centre_y
        photo.crop_scale = scale
        Session.commit()

    @QtCore.pyqtSlot(int, str)
    def update_photo_opinion(self, id, opinion):
        photo = Photo.by_id(id)
        photo.opinion = str(opinion)
        Session.commit()

    @QtCore.pyqtSlot(int, float)
    def update_photo_rotation(self, id, angle):
        photo = Photo.by_id(id)
        photo.rotate = angle
        Session.commit()

class DBUpdater(QtCore.QObject):
    sig_update_photo_crop = QtCore.pyqtSignal(int, float, float, float)
    sig_update_photo_opinion = QtCore.pyqtSignal(int, str)
    sig_update_photo_rotation = QtCore.pyqtSignal(int, float)
    
    def __init__(self):
        super(QtCore.QObject, self).__init__()
        
        self.worker = WorkerThread()
        self.updater = DBUpdateWorker()
        self.updater.moveToThread(self.worker)

        self.sig_update_photo_crop.connect(self.updater.update_photo_crop)
        self.sig_update_photo_opinion.connect(self.updater.update_photo_opinion)
        self.sig_update_photo_rotation.connect(self.updater.update_photo_rotation)
        
        self.worker.start()
        
    def update_photo_crop(self, id, centre_x, centre_y, scale):
        self.sig_update_photo_crop.emit(id, centre_x, centre_y, scale)
        
    def update_photo_opinion(self, id, opinion):
        self.sig_update_photo_opinion.emit(id, opinion)
        
    def update_photo_rotation(self, id, angle):
        self.sig_update_photo_rotation.emit(id, angle)
