from PyQt4 import QtCore, QtGui, QtNetwork
from collections import OrderedDict
from ef.netlib import qt_page_get
from ef.threads import WorkerThread
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
        f = open(filename, 'wb')
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

        if filename is None or url is None:
            return None
            
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

class SignalGroup(QtCore.QObject):
    '''A SignalGroup will emit its fire signal once, after all the
    signals in the group have been emitted at least once. Repeat
    emissions and signal arguments are ignored'''
    
    fire = QtCore.pyqtSignal()
    def __init__(self, *signals):
        QtCore.QObject.__init__(self)
        self.pending = set()
        self.fired = False
        for signal in signals:
            self.pending.add(signal)
            signal.connect(lambda *args, **kwargs: self.handle_signal(signal))

    def handle_signal(self, signal):
        self.pending.discard(signal)
        if not self.fired and not self.pending:
            self.fire.emit()
            self.fired = True
