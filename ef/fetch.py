from PyQt4 import QtCore
from ef.db import Person, Photo, Event, Registration, FindPhotos, Batch, FetchedPhoto
from ef.parser import EFDelegateParser
import traceback
import time
from ef.threads import thread_registry
from ef.login import LoginTask, LoginError
from ef.nettask import NetFuncs
from ef.task import Task

class PersonDBParser(EFDelegateParser):
    def __init__(self, progress, batch):
        EFDelegateParser.__init__(self)
        self.progress = progress
        self.batch = batch

    def handle_person(self, person):
        Person.upsert({'id': person['Person ID'],
                       'firstname': person['Firstname'],
                       'lastname': person['Lastname'],
                       'title': person['Salutation'],
                       'fullname': person['Full Name'],
                       'last_checked_at': time.time(),
                       }, batch=self.batch)
        self.progress.emit('Updated %d people' % len(self.people), 0, 0)

    def handle_event(self, event_id, event_name):
        Event.upsert({'id': event_id,
                      'name': event_name,
                      }, batch=self.batch)

    def handle_registration(self, person, event_id):
        data = person['events'][event_id]
        Registration.upsert({'person_id': person['Person ID'],
                             'event_id': event_id,
                             'attendee_type': data['Type of Attendee'],
                             'booking_ref': data['Booking Ref'],
                             'booker_firstname': data['Bookers Firstname'],
                             'booker_lastname': data['Bookers lastname'],
                             'booker_email': data['Bookers Email'],
                             }, batch=self.batch)

def catcherror(func):
    def wrapped(self, *args, **kwargs):
        try:
            func(self, *args, **kwargs)
        except:
            self.error.emit(traceback.format_exc())
    return wrapped

class ReportTask(Task, NetFuncs):
    def __init__(self, worker):
        Task.__init__(self)
        NetFuncs.__init__(self)

        self.worker = worker
    
    def task(self):
        self.batch = Batch()
        self.parser = PersonDBParser(self.worker.progress, self.batch)

        self.worker.progress.emit('Running report', 0, 0)
        soup = yield self.get('https://www.eventsforce.net/libdems/backend/home/dynaRepRun.csp?profileID=62')
    
        img = soup.find('img', title='Export to Excel')
        if img is None:
            raise FetchError("Failed to parse response from eventsforce (didn't have Export link)")
        link = img.parent

        self.worker.progress.emit('Downloading results', 0, 0)

        self.report_op = self.get_raw(link['href'])
        self.report_op.reply.readyRead.connect(self.report_get_data)
        yield self.report_op

        self.parser.close()

        self.batch.progress.connect(self.handle_commit_progress)
        self.batch.finish()

        yield self.wait(self.batch)

    @catcherror
    def report_get_data(self):
        self.parser.feed(self.report_op.result())

    def handle_commit_progress(self, cur, max):
        self.worker.progress.emit('Saving people', cur, max)

class PhotosTask(Task, NetFuncs):
    def __init__(self, worker, which, batch):
        Task.__init__(self)
        NetFuncs.__init__(self)

        self.worker = worker
        self.which = which
        self.batch = batch

    def task(self):
        find_photos = FindPhotos(self.which)
        find_photos.run()
        self.worker.progress.emit('Finding photos', 0, 0)
        yield self.wait(find_photos)
        
        self.db_tasks = []

        people = find_photos.result()

        for i, person_id in enumerate(people):
            self.worker.progress.emit('Finding photos', i, len(people))
            soup = yield self.get('https://www.eventsforce.net/libdems/backend/home/codEditMain.csp?codReadOnly=1&personID=%d&curPage=1' % person_id)
            img = soup.find('img', title='Picture Profile')
            if img is not None:
                url = str(self.current.resolve_url(img['src']).toString())
                fetched = FetchedPhoto(person_id, url, self.batch)
                self.db_tasks.append(fetched)
                fetched.run()

        self.batch.finish()
        self.batch.progress.connect(self.handle_commit_progress)

    def handle_commit_progress(self, cur, max):
        self.worker.progress.emit('Saving photo URLs', cur, max)

class FetchWorker(QtCore.QObject):
    completed = QtCore.pyqtSignal()
    error = QtCore.pyqtSignal(str)
    progress = QtCore.pyqtSignal(str, int, int)
    
    def __init__(self):
        super(QtCore.QObject, self).__init__()

        self.tasks = None

    @QtCore.pyqtSlot(bool, str, str, str)
    @catcherror
    def start_fetch(self, fetch_report, fetch_photos, username, password):
        self.batch = Batch()
        self.tasks = tasks = [LoginTask(username, password)]
        if fetch_report:
            tasks.append(ReportTask(self))
        if fetch_photos != 'none':
            tasks.append(PhotosTask(self, fetch_photos, self.batch))

        prev = None
        for task in tasks:
            if prev is not None:
                prev.task_finished.connect(task.start_task)
            task.task_exception.connect(self.handle_exception)
            prev = task
        prev.task_finished.connect(self.batch.finish)
        self.batch.finished.connect(self.completed)

        self.progress.emit('Logging in', 0, 0)
        tasks[0].start_task()

    def handle_exception(self, e, msg):
        if isinstance(e, LoginError):
            self.error.emit(str(e))
        else:
            self.error.emit(msg)

        if self.tasks is not None:
            for task in self.tasks:
                task.abort()

class Fetcher(QtCore.QObject):
    sig_start_fetch = QtCore.pyqtSignal(bool, str, str, str)
    
    def __init__(self):
        super(QtCore.QObject, self).__init__()
        
        self.fetcher = FetchWorker()
        self.fetcher.moveToThread(thread_registry.get('network'))

        self.sig_start_fetch.connect(self.fetcher.start_fetch)

        # This is an interesting idiom: copy the bound signals into
        # this object, so clients can just connect to them and hear
        # directly from the fetcher
        self.completed = self.fetcher.completed
        self.error = self.fetcher.error
        self.progress = self.fetcher.progress
        
    def start_fetch(self, fetch_report, fetch_photos, username, password):
        self.sig_start_fetch.emit(fetch_report, fetch_photos, username, password)
