from PyQt4 import QtCore, QtGui
from ef.nettask import NetFuncs
from ef.task import Task
from ef.threads import thread_registry
from ef.db import Photo
from collections import OrderedDict
import os
import sys
from PIL import Image
from StringIO import StringIO

class PhotoDownload(Task, NetFuncs):
    def __init__(self, id, url, filename):
        Task.__init__(self)
        NetFuncs.__init__(self)

        self.id = id
        self.url = url
        self.filename = filename

    def task(self):
        self.data = yield self.get_raw(self.url)

        # Collecting the size here has the neat side-effect that we'll
        # throw an exception if the data isn't a valid image, so we
        # won't write complete junk (like an HTML error message) to
        # the cache
        buf = StringIO(str(self.data))
        image = Image.open(buf)
        width, height = image.size
        Photo.upsert({'id': self.id, 'width': width, 'height': height})

    def write_file(self):
        f = open(self.filename, 'wb')
        f.write(self.data)
        f.close()

class PhotoDownloadWorker(QtCore.QObject):
    ready = QtCore.pyqtSignal(int)
    error = QtCore.pyqtSignal(int, str)
    queue_size = QtCore.pyqtSignal(int)

    def __init__(self):
        QtCore.QObject.__init__(self)
        self.queue = {'urgent': None, 'normal': OrderedDict(), 'background': OrderedDict()}
        self.current_task = None

    @QtCore.pyqtSlot(int, str, str, bool, bool, bool)
    def download_photo(self, id, url, filename, refresh, urgent, background):
        if not refresh and os.path.exists(filename):
            self.ready.emit(id)
            return

        # Don't bother repeating the one we're downloading right now,
        # that's going to get a signal in the near future - unless we
        # asked for refresh, in which case the current download may be
        # too old
        if self.current_task is not None and self.current_task.id == id and not refresh:
            return

        queue = self.queue['background' if background else 'normal']
        queue[id] = {'id': id, 'url': url, 'filename': filename}
        if urgent:
            self.queue['urgent'] = id

        # Flatten out duplicate background loads, however they got here (lots of ways that can happen)
        if self.queue['normal'].has_key(id):
            self.queue['background'].pop(id, None)

        self.start_task()

    def start_task(self):
        self.queue_size.emit(len(self.queue['normal']) + len(self.queue['background']) + 0 if self.current_task is None else 1)

        if self.current_task is not None:
            return

        item = None
        if self.queue['urgent'] is not None:
            id = self.queue['urgent']
            item = self.queue['normal'].pop(id, None)
            self.queue['urgent'] = None
        if item is None:
            for queue in [self.queue['normal'], self.queue['background']]:
                if queue:
                    id, item = queue.popitem(last=False)
                    break
        if item is None:
            return

        self.current_task = PhotoDownload(item['id'], item['url'], item['filename'])
        self.current_task.task_finished.connect(self.handle_task_finished)
        self.current_task.task_exception.connect(self.handle_task_exception)
        self.current_task.start_task()

    def cleanup_task(self):
        # Hold GC for one pass
        self.last_task = self.current_task
        self.current_task = None
        
    def handle_task_finished(self):
        if self.current_task is None:
            return
        # This is a hack to work around python bug #14432 which prevents use of open() in the task coroutine
        self.current_task.write_file()
        self.ready.emit(self.current_task.id)
        self.cleanup_task()
        self.start_task()

    def handle_task_exception(self, e, str):
        print e, self.current_task.url
        if self.current_task is None:
            return
        self.error.emit(self.current_task.id, str)
        self.cleanup_task()
        self.start_task()

class PhotoDownloader(QtCore.QObject):
    sig_download_photo = QtCore.pyqtSignal(int, str, str, bool, bool, bool)

    def __init__(self):
        super(QtCore.QObject, self).__init__()
        
        self.downloader = PhotoDownloadWorker()
        self.downloader.moveToThread(thread_registry.get('network'))

        self.sig_download_photo.connect(self.downloader.download_photo)

        self.ready = self.downloader.ready
        self.error = self.downloader.error
        self.queue_size = self.downloader.queue_size

        self.queue_size.connect(self.update_queue_size)
        self.latest_queue_size = None
        
    def download_photo(self, id, url, filename, refresh=False, urgent=False, background=False):
        self.sig_download_photo.emit(id, url, filename, refresh, urgent, background)

    def update_queue_size(self, size):
        self.latest_queue_size = size

    def get_queue_size(self):
        return self.latest_queue_size
