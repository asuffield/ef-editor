from PyQt4 import QtCore, QtGui
from ef.threads import WorkerThread
from ef.lib import LimitedSizeDict
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
    def load_image(self, id, filename, scale_size):
        if not os.path.exists(filename):
            self.photo_fail(id, 'File not found: %s' % filename)
            return

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

class PhotoCache(QtCore.QObject):
    start_loading = QtCore.pyqtSignal(int, str, QtCore.QSize)
    
    def __init__(self, downloader, limit, scale_size=QtCore.QSize()):
        super(QtCore.QObject, self).__init__()

        self.cache = LimitedSizeDict(size_limit=limit)
        self.scale_size = scale_size
        self.loading = {}
        self.handlers = {}

        self.downloader = downloader
        self.worker = WorkerThread()
        self.loader = PhotoLoader()
        self.loader.moveToThread(self.worker)

        self.loader.photo_ready.connect(self._photo_ready)
        self.loader.photo_fail.connect(self._photo_fail)
        self.downloader.ready.connect(self._download_ready)
        self.downloader.error.connect(self._download_error)
        self.start_loading.connect(self.loader.load_image)

        self.worker.start()
        
    def load_image(self, id, filename, url, ready_cb=None, fail_cb=None, refresh=False, urgent=False, background=False):
        if id in self.loading:
            return

        if self.cache.has_key(id):
            self._bump_cache_entry(id)
            if ready_cb is not None:
                ready_cb(id, self.cache[id])
            return

        if filename is None:
            return None

        self.handlers.setdefault(id, []).append({'ready': ready_cb, 'fail': fail_cb})
        self.loading[id] = filename

        if url is None:
            self.start_loading.emit(id, filename, self.scale_size)
        else:
            self.downloader.download_photo(id, url, filename, refresh=refresh, urgent=urgent, background=background)

    def _bump_cache_entry(self, id):
        # This bumps an item up to the head of the cache
        if self.cache.has_key(id):
            self.cache[id] = self.cache.pop(id)

    def peek_image(self, id):
        self._bump_cache_entry(id)
        return self.cache.get(id, None)

    def _cleanup_after_load(self, id):
        self.handlers.pop(id, None)
        self.loading.pop(id, None)

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

    def _download_ready(self, id):
        if not self.loading.has_key(id):
            return
        self.start_loading.emit(id, self.loading[id], self.scale_size)

    def _download_error(self, id, error):
        for handler in self.handlers.get(id, []):
            if handler['fail'] is not None:
                handler['fail'](id, error)
        self._cleanup_after_load(id)
