from PyQt4 import QtCore

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
