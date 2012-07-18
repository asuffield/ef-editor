from PyQt4 import QtCore
from ef.login import LoginTask, LoginError

class TryLogin(QtCore.QObject):
    completed = QtCore.pyqtSignal()
    error = QtCore.pyqtSignal(str)
    
    def __init__(self):
        QtCore.QObject.__init__(self)

    @QtCore.pyqtSlot(str, str)
    def start_login(self, username, password):
        self.task = LoginTask(username, password)
        self.task.task_finished.connect(self.completed)
        self.task.task_exception.connect(self.handle_exception)

        self.task.start_task()

    def handle_exception(self, e, msg):
        if isinstance(e, LoginError):
            self.error.emit(str(e))
        else:
            self.error.emit(msg)
