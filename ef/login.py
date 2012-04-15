from ef.nettask import NetFuncs
from ef.task import Task
from PyQt4 import QtCore
import re

class LoginError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return self.value

class LoginTask(Task, NetFuncs):
    completed = QtCore.pyqtSignal(bool)
    error = QtCore.pyqtSignal(str)

    def __init__(self, username, password):
        Task.__init__(self)
        NetFuncs.__init__(self)

        self.username = username
        self.password = password

    def task(self):
        soup = yield self.get('https://www.eventsforce.net/libdems/backend/home/login.csp')
        soup = yield self.submit_form(soup.form, {'txtUsername': self.username, 'txtPassword': self.password})
        if soup.find(text=re.compile('Invalid logon')) is not None:
            raise LoginError('Invalid logon')
        if soup.find('h2') is None:
            raise LoginError('Got incomprehensible page from eventsforce after login')
