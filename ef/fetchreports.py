from PyQt4 import QtCore
import time
import re
from ef.login import LoginTask, LoginError
from ef.nettask import NetFuncs
from ef.task import Task, TaskList

# This isn't used right now, fetches the list of reports
class ReportListTask(Task, NetFuncs):
    def __init__(self):
        Task.__init__(self)
        NetFuncs.__init__(self)

        self.reports = []

    def task(self):
        soup = yield self.get('https://www.eventsforce.net/libdems/backend/home/dynaRepStart.csp')
        for report_tag in soup.find_all('td', text=re.compile(r'#export')):
            report_name = unicode(report_tag.string).replace('#export', '').strip()
            report_link = None
            for button_tag in report_tag.find_next_siblings('td'):
                link = button_tag.find('a', href=re.compile(r'dynaRepRun\.csp'))
                if link:
                    report_link = link['href']
            m = re.search(r'profileID=(\d+)', report_link)
            if m is not None:
                report_id = m.group(1)
                try:
                    self.reports.append((report_name, int(report_id)))
                except TypeError:
                    pass

# Currently using this one, fetches parameters from report 69
class ReportParamsTask(Task, NetFuncs):
    def __init__(self):
        Task.__init__(self)
        NetFuncs.__init__(self)

        self.reports = []

    def task(self):
        soup = yield self.get('https://www.eventsforce.net/libdems/backend/home/dynaRepRun.csp?profileID=69')
        select = soup.find('select', {'name': re.compile(r'^value1_')})
        for option in select.find_all('option'):
            if not option.has_key('value'):
                continue
            self.reports.append((option.text, int(option['value'])))

class ReportsFetchWorker(QtCore.QObject):
    completed = QtCore.pyqtSignal(list)
    error = QtCore.pyqtSignal(str)
    
    def __init__(self):
        QtCore.QObject.__init__(self)

    @QtCore.pyqtSlot(str, str)
    def start_fetch(self, username, password):
        self.reportslist_task = ReportParamsTask()
        self.task = TaskList(LoginTask(username, password), self.reportslist_task)
        self.task.task_finished.connect(self.handle_finished)
        self.task.task_exception.connect(self.handle_exception)

        self.task.start_task()

    def handle_finished(self):
        self.completed.emit(self.reportslist_task.reports)

    def handle_exception(self, e, msg):
        if isinstance(e, LoginError):
            self.error.emit(str(e))
        else:
            self.error.emit(msg)

class ReportsFetcher(QtCore.QObject):
    sig_start_fetch = QtCore.pyqtSignal(str, str)
    
    def __init__(self):
        super(QtCore.QObject, self).__init__()
        
        self.fetcher = ReportsFetchWorker()
        #self.fetcher.moveToThread(thread_registry.get('network'))

        self.sig_start_fetch.connect(self.fetcher.start_fetch)

        self.completed = self.fetcher.completed
        self.error = self.fetcher.error
        
    def start_fetch(self, username, password):
        self.sig_start_fetch.emit(username, password)
