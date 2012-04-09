from PyQt4 import QtCore, QtGui, QtNetwork
from ef.lib import WorkerThread
from ef.db import Person, Photo, Event, Registration, FindPhotos, Batch
from ef.parser import EFDelegateParser
from bs4 import BeautifulSoup
import traceback
import time
from ef.netlib import qt_form_post, qt_page_get, qt_reply_charset, qt_readall_charset, qt_relative_url

import thread

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

class FetchTask(QtCore.QObject):
    start = QtCore.pyqtSignal()
    error = QtCore.pyqtSignal(str)
    finished = QtCore.pyqtSignal()

    def __init__(self, worker):
        QtCore.QObject.__init__(self)
        self.worker = worker
        self.start.connect(self.main)

    def run(self):
        pass

    def main(self):
        self.run()

    def abort(self):
        pass

    def reply_has_error(self, reply):
        if reply.error() != QtNetwork.QNetworkReply.NoError:
            self.error.emit(reply.errorString())
            return True
        else:
            return False

class LoginTask(FetchTask):
    def __init__(self, worker, username, password):
        FetchTask.__init__(self, worker)
        self.username = username
        self.password = password
        self.reply = None

    @catcherror
    def run(self):
        self.worker.progress.emit('Logging in', 0, 0)
        reply = qt_form_post(self.worker.manager,
                             'https://www.eventsforce.net/libdems/backend/home/login2.csp',
                             {'txtUsername': self.username,
                              'txtPassword': self.password,
                              })
        reply.finished.connect(self.login_finished)
        self.reply = reply

    def login_finished(self):
        if not self.reply_has_error(self.reply):
            self.finished.emit()

    def abort(self):
        if self.reply is not None and not self.reply.isFinished():
            self.reply.finished.disconnect()
            self.reply.abort()

class ReportTask(FetchTask):
    def __init__(self, worker):
        FetchTask.__init__(self, worker)
        self.run_reply = None
        self.get_reply = None
        self.charset = None
    
    @catcherror
    def run(self):
        self.batch = Batch()
        self.parser = PersonDBParser(self.worker.progress, self.batch)

        self.worker.progress.emit('Running report', 0, 0)
        reply = qt_page_get(self.worker.manager,
                            'https://www.eventsforce.net/libdems/backend/home/dynaRepRun.csp?profileID=62')

        reply.finished.connect(self.report_run_finished)
        self.run_reply = reply

    @catcherror
    def report_run_finished(self):
        if self.reply_has_error(self.run_reply):
            return

        redirect = self.run_reply.attribute(QtNetwork.QNetworkRequest.RedirectionTargetAttribute)
        if redirect.isValid():
            url = qt_relative_url(self.run_reply, redirect.toString())
            reply = qt_page_get(self.worker.manager, url)
            reply.finished.connect(self.report_run_finished)
            self.run_reply = reply
            return

        charset = qt_reply_charset(self.run_reply)
        soup = BeautifulSoup(qt_readall_charset(self.run_reply, charset))

        img = soup.find('img', title='Export to Excel')
        if img is None:
            self.error.emit("Failed to parse response from eventsforce (didn't have Export link)")
            return
        link = img.parent

        self.worker.progress.emit('Downloading results', 0, 0)

        url = qt_relative_url(self.run_reply, link['href'])
        reply = qt_page_get(self.worker.manager, url)
        reply.metaDataChanged.connect(self.report_get_headers)
        reply.readyRead.connect(self.report_get_data)
        reply.finished.connect(self.report_get_finished)

        self.run_reply = None
        self.get_reply = reply

    @catcherror
    def report_get_headers(self):
        if self.reply_has_error(self.get_reply):
            return

        self.charset = qt_reply_charset(self.get_reply)

    @catcherror
    def report_get_data(self):
        if self.reply_has_error(self.get_reply):
            return

        self.parser.feed(qt_readall_charset(self.get_reply, self.charset))

    @catcherror
    def report_get_finished(self):
        if self.reply_has_error(self.get_reply):
            return

        self.parser.close()
        self.batch.finished.connect(self.finished)
        self.batch.progress.connect(self.handle_commit_progress)
        self.batch.finish()

    def handle_commit_progress(self, cur, max):
        self.worker.progress.emit('Saving people', cur, max)

    def abort(self):
        if self.run_reply is not None and not self.run_reply.isFinished():
            self.run_reply.finished.disconnect()
            self.run_reply.abort()
        if self.get_reply is not None and not self.get_reply.isFinished():
            self.get_reply.finished.disconnect()
            self.get_reply.abort()

class PhotosTask(FetchTask):
    def __init__(self, worker, which):
        FetchTask.__init__(self, worker)
        self.get_reply = None
        self.which = which

    @catcherror
    def run(self):
        self.find_photos = FindPhotos(self.which)
        self.find_photos.results.connect(self.photos_ready)
        self.find_photos.run()
        self.worker.progress.emit('Finding photos', 0, 0)

    @catcherror
    def photos_ready(self, ids):
        self.people = ids
        self.worker.progress.emit('Finding photos', 0, len(self.people))
        self.i = 0
        self.run_person()

    def run_person(self):
        if self.i >= len(self.people):
            self.finished.emit()
            return

        reply = qt_page_get(self.worker.manager,
                            'https://www.eventsforce.net/libdems/backend/home/codEditMain.csp?codReadOnly=1&personID=%d&curPage=1' % self.people[self.i])
        reply.finished.connect(self.get_finished)
        self.get_reply = reply

    @catcherror
    def get_finished(self):
        if self.reply_has_error(self.get_reply):
            return

        charset = qt_reply_charset(self.get_reply)
        soup = BeautifulSoup(qt_readall_charset(self.get_reply, charset))

        img = soup.find('img', title='Picture Profile')
        if img is not None:
            url = str(qt_relative_url(self.get_reply, img['src']).toString())
            Photo.fetched(self.people[self.i], url)
        self.worker.progress.emit('Finding photos', self.i, len(self.people))

        self.i = self.i + 1
        self.run_person()

    def abort(self):
        if self.get_reply is not None and not self.get_reply.isFinished():
            self.get_reply.finished.disconnect()
            self.get_reply.abort()

class FetchWorker(QtCore.QObject):
    completed = QtCore.pyqtSignal()
    error = QtCore.pyqtSignal(str)
    progress = QtCore.pyqtSignal(str, int, int)
    
    def __init__(self):
        super(QtCore.QObject, self).__init__()

        self.tasks = None
        self.manager = None

    @QtCore.pyqtSlot(bool, str, str, str)
    @catcherror
    def start_fetch(self, fetch_report, fetch_photos, username, password):
        if self.manager is None:
            self.manager = QtNetwork.QNetworkAccessManager()

        self.tasks = tasks = [LoginTask(self, username, password)]
        if fetch_report:
            tasks.append(ReportTask(self))
        if fetch_photos != 'none':
            tasks.append(PhotosTask(self, fetch_photos))

        prev = None
        for task in tasks:
            if prev is not None:
                prev.finished.connect(task.start)
            task.error.connect(self.handle_error)
            prev = task
        prev.finished.connect(self.completed)
        tasks[0].start.emit()

    def handle_error(self, err):
        self.error.emit(err)
        if self.tasks is not None:
            for task in self.tasks:
                task.abort()

class Fetcher(QtCore.QObject):
    sig_start_fetch = QtCore.pyqtSignal(bool, str, str, str)
    
    def __init__(self):
        super(QtCore.QObject, self).__init__()
        
        self.worker = WorkerThread()
        self.fetcher = FetchWorker()
        self.fetcher.moveToThread(self.worker)

        self.sig_start_fetch.connect(self.fetcher.start_fetch)

        # This is an interesting idiom: copy the bound signals into
        # this object, so clients can just connect to them and hear
        # directly from the fetcher
        self.completed = self.fetcher.completed
        self.error = self.fetcher.error
        self.progress = self.fetcher.progress
        
        self.worker.start()

    def start_fetch(self, fetch_report, fetch_photos, username, password):
        self.sig_start_fetch.emit(fetch_report, fetch_photos, username, password)
