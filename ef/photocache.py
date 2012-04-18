from PyQt4 import QtCore, QtGui
from ef.threads import WorkerThread
from ef.lib import LRUCache
from ef.image import PhotoImage
import os
import traceback
import Image

max_editing_width = 1200
max_editing_height = 1600

class ThumbnailLoader(QtCore.QObject):
    ready = QtCore.pyqtSignal(int, QtGui.QImage)
    fail = QtCore.pyqtSignal(int, str)

    @QtCore.pyqtSlot(int, str)
    def load_image(self, id, filename):
        if not os.path.exists(filename):
            self.fail.emit(id, 'File not found: %s' % filename)
            return

        reader = QtGui.QImageReader(filename)
        image = reader.read()

        if image.isNull():
            self.fail.emit(id, reader.errorString())
        else:
            image = image.scaled(QtCore.QSize(60, 80), QtCore.Qt.KeepAspectRatio)
            self.ready.emit(id, image)

class ImageLoader(QtCore.QObject):
    ready = QtCore.pyqtSignal(int, list)
    fail = QtCore.pyqtSignal(int, str)

    @QtCore.pyqtSlot(int, str)
    def load_image(self, id, filename):
        if not os.path.exists(filename):
            self.fail.emit(id, 'File not found: %s' % filename)
            return

        try:
            image = Image.open(str(filename)).convert('RGBA')

            # Get outsized images down to working dimensions, so that we aren't hauling around megabytes of excess pixels
            width, height = image.size
            want_scale = False
            while width > max_editing_width or height > max_editing_height:
                width = width / 2
                height = height / 2
                want_scale = True
            if want_scale:
                image = image.resize((width,height))

            self.ready.emit(id, [image])
        except Exception:
            self.fail.emit(id, traceback.format_exc())

class PhotoCacheBase(QtCore.QObject):
    def __init__(self, downloader, limit):
        QtCore.QObject.__init__(self)

        self.cache = LRUCache(size_limit=limit)
        self.loading = {}
        self.handlers = {}

        self.downloader = downloader
        self.downloader.ready.connect(self._download_ready)
        self.downloader.error.connect(self.fail)

    def load_image(self, id, filename, url, ready_cb=None, fail_cb=None, refresh=False, urgent=False, background=False):
        if id in self.loading:
            return

        if not refresh:
            photo = self.cache[id]
            if photo is not None:
                if ready_cb is not None:
                    ready_cb(id, photo)
                return

        if filename is None:
            return None

        self.handlers.setdefault(id, []).append({'ready': ready_cb, 'fail': fail_cb})
        self.loading[id] = filename

        if url is None:
            self.load(id)
        else:
            self.downloader.download_photo(id, url, filename, refresh=refresh, urgent=urgent, background=background)

    def load(self, id):
        pass

    def ready(self, id, photo):
        self.cache[id] = photo
        for handler in self.handlers.get(id, []):
            if handler['ready'] is not None:
                handler['ready'](id, photo)
        self.cleanup_after_load(id)

    def fail(self, id, err):
        for handler in self.handlers.get(id, []):
            if handler['fail'] is not None:
                handler['fail'](id, err)
        self.cleanup_after_load(id)

    def peek_image(self, id):
        return self.cache[id]

    def cleanup_after_load(self, id):
        self.handlers.pop(id, None)
        self.loading.pop(id, None)

    def _download_ready(self, id):
        if id in self.loading:
            self.load(id)

class ThumbnailCache(PhotoCacheBase):
    start_loading = QtCore.pyqtSignal(int, str)
    
    def __init__(self, *args, **kwargs):
        self.scale_size = kwargs.pop('scale_size', QtCore.QSize())
        
        PhotoCacheBase.__init__(self, *args, **kwargs)

        self.worker = WorkerThread()
        self.loader = ThumbnailLoader()
        self.loader.moveToThread(self.worker)

        self.loader.ready.connect(self.photo_ready)
        self.loader.fail.connect(self.fail)
        self.start_loading.connect(self.loader.load_image)

        self.worker.please_exit.connect(self.worker.exit)
        self.worker.start()

    def load(self, id):
        self.start_loading.emit(id, self.loading[id])

    def photo_ready(self, id, image):
        pixmap = QtGui.QPixmap.fromImage(image)
        self.ready(id, pixmap)

class PhotoImageCache(PhotoCacheBase):
    start_loading = QtCore.pyqtSignal(int, str)
    
    def __init__(self, *args, **kwargs):
        PhotoCacheBase.__init__(self, *args, **kwargs)

        self.worker = WorkerThread()
        self.loader = ImageLoader()
        self.loader.moveToThread(self.worker)

        self.loader.ready.connect(self.photo_ready)
        self.loader.fail.connect(self.fail)
        self.start_loading.connect(self.loader.load_image)

        self.worker.please_exit.connect(self.worker.exit)
        self.worker.start()

    def load(self, id):
        self.start_loading.emit(id, self.loading[id])

    def photo_ready(self, id, image_list):
        self.ready(id, PhotoImage(image_list[0]))
