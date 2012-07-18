from ef.nettask import NetFuncs
from ef.task import Task
from PyQt4 import QtCore
from bs4 import SoupStrainer
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
        soup = yield self.get('https://www.eventsforce.net/libdems/backend/home/login.csp', parse_only=SoupStrainer('form'))

        login_strainer = SoupStrainer(['script', 'title', 'span'])

        soup = yield self.submit_form(soup.form, {'txtUsername': self.username, 'txtPassword': self.password}, parse_only=login_strainer)

        # Follow hideous javascript redirect that they snuck into the login sequence
        m = None
        for script in soup.find_all('script'):
            m = re.search(r'var redirectURL="(.*)"', script.text)
            if m:
                link = m.group(1)
                soup = yield self.get(link, parse_only=login_strainer)
                break

        if soup.find(text=re.compile('Invalid logon')) is not None:
            raise LoginError('Invalid logon')
        if soup.title is None:
            raise LoginError('Got incomprehensible page from eventsforce after login')
        if soup.title.text != 'Liberal Democrats':
            raise LoginError('Got unexpected page title "%s" from eventsforce after login' % soup.title.text)
