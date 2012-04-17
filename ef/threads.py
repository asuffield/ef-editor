from PyQt4 import QtCore

class ThreadRegistry(QtCore.QObject):
    def __init__(self):
        QtCore.QObject.__init__(self)
        self.threads = {}

    def add(self, thread, name):
        self.threads[name] = thread

    def wait_all(self):
        for thread in self.threads.itervalues():
            thread.wait()
        self.threads = {}

    def shutdown(self, rc):
        for thread in self.threads.itervalues():
            thread.please_exit.emit(rc)

    def get(self, name):
        return self.threads[name]

thread_registry = ThreadRegistry()

class WorkerThread(QtCore.QThread):
    please_exit = QtCore.pyqtSignal(int)
    
    def __init__(self, *args, **kwargs):
        id = kwargs.pop('name', self)
        QtCore.QThread.__init__(self, *args, **kwargs)
        thread_registry.add(self, id)
