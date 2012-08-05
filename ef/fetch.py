from PyQt4 import QtCore
from ef.db import Person, Photo, Event, Registration, Batch, FetchedPhoto
from ef.parser import EFDelegateParser
import traceback
import time
import re
from ef.login import LoginTask, LoginError
from ef.nettask import NetFuncs
from ef.task import Task, TaskList
from bs4 import SoupStrainer

class FetchError(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return self.msg

class PersonDBParser(EFDelegateParser):
    def __init__(self, progress, batch):
        EFDelegateParser.__init__(self)
        self.progress = progress
        self.batch = batch

    def handle_person(self, person):
        firstname = person['Firstname'] or person['common first name']
        fullname = filter(lambda x: len(x) > 0, [person['Salutation'].strip(), firstname.strip(), person['Lastname'].strip()])
        Person.upsert({'id': person['Person ID'],
                       'firstname': firstname.strip(),
                       'lastname': person['Lastname'].strip(),
                       'title': person['Salutation'].strip(),
                       'fullname': ' '.join(fullname),
                       'police_status': person['EF_Application Status'],
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
                             }, batch=self.batch)

def catcherror(func):
    def wrapped(self, *args, **kwargs):
        try:
            func(self, *args, **kwargs)
        except:
            self.error.emit(traceback.format_exc())
    return wrapped

class ReportTask(Task, NetFuncs):
    def __init__(self, event, since, progress):
        Task.__init__(self)
        NetFuncs.__init__(self)

        self.progress = progress
        self.event = event
        self.since = since
        #self.f = open('output.tmp', 'w')
    
    def task(self):
        self.batch = Batch()
        self.parser = PersonDBParser(self.progress, self.batch)

        self.progress.emit('Running report', 0, 0)
        soup = yield self.get('https://www.eventsforce.net/libdems/backend/home/dynaRepRun.csp?profileID=69', timeout=None)

        date_id = None
        event_id = None
        for elem in soup.find_all('input', {'type': 'hidden', 'name': re.compile(r'^criteriaDescription')}):
            m = re.match(r'^criteriaDescription_(\d+)$', elem['name'])
            if m:
                this_id = m.group(1)
            else:
                continue
            if elem['value'] == 'Amendment Date':
                date_id = this_id
            elif elem['value'] == 'In Event':
                event_id = this_id

        if date_id is None or event_id is None:
            raise FetchError("Failed to parse form from eventsforce (couldn't find in report parameters: %s, %s)" % (date_id, event_id))

        soup = yield self.submit_form(soup.form, {'value1_%s' % date_id: self.since.toString('dd-MMM-yyyy'),
                                                  'value2_%s' % date_id: QtCore.QDate.currentDate().toString('dd-MMM-yyyy'),
                                                  'value1_%s' % event_id: str(self.event),
                                                  })

        img = soup.find('img', title='Export to Excel')
        if img is None:
            raise FetchError("Failed to parse response from eventsforce (didn't have Export link)")
        link = img.parent

        self.progress.emit('Downloading results', 0, 0)

        self.report_op = self.get_raw(link['href'], timeout=120)
        self.report_op.reply.readyRead.connect(self.report_get_data)
        yield self.report_op

        self.parser.close()

        self.batch.progress.connect(self.handle_commit_progress)
        self.batch.finish()

        yield self.wait(self.batch)

    def report_get_data(self):
        data = self.report_op.result()
        #self.f.write(unicode(data).encode('utf-8'))
        self.parser.feed(data)

    def handle_commit_progress(self, cur, max):
        self.progress.emit('Saving people', cur, max)

class PhotosTask(Task, NetFuncs):
    def __init__(self, progress, people, batch):
        Task.__init__(self)
        NetFuncs.__init__(self)

        self.progress = progress
        self.people = people
        self.batch = batch

    def task(self):
        self.db_tasks = []

        image_strainer = SoupStrainer(['img'])

        for i, person in enumerate(self.people):
            self.progress.emit('Finding photos', i, len(self.people))
            soup = yield self.get('https://www.eventsforce.net/libdems/backend/home/codEditMain.csp?codReadOnly=1&personID=%d&curPage=1' % person.id, parse_only=image_strainer)
            img = soup.find('img', title='Picture Profile')
            if img is not None:
                url = QtCore.QUrl()
                url.setEncodedUrl(img['src'])
                fetched = FetchedPhoto(person, str(self.current.resolve_url(url).toEncoded()), self.batch)
                self.db_tasks.append(fetched)

class CategoryTask(Task, NetFuncs):
    def __init__(self, progress, regs, batch):
        Task.__init__(self)
        NetFuncs.__init__(self)

        self.progress = progress
        self.regs = regs
        self.batch = batch

    def task(self):
        category_strainer = SoupStrainer(['tr', 'td', 'img'])

        for i, reg in enumerate(self.regs):
            self.progress.emit('Finding missing registration categories', i, len(self.regs))
            soup = yield self.get('https://www.eventsforce.net/libdems/backend/home/codEditMain.csp?codReadOnly=1&personID=%d&eventID=%d&curPage=1' % (reg.person_id, reg.event_id), parse_only=category_strainer)
            img = soup.find('img', src=re.compile(r'.*/backend/tick.gif$'))
            if img is not None:
                img_td = img.find_parent('td')
                category_td = img_td.find_previous_sibling('td')
                category = category_td.string
                if category is not None:
                    reg.update_category(category.strip(), batch=self.batch)

class BatchFinishTask(Task):
    def __init__(self, progress, batch):
        Task.__init__(self)

        self.progress = progress
        self.batch = batch

    def task(self):
        self.batch.progress.connect(self.handle_commit_progress)
        self.batch.finish()
        return None

    def handle_commit_progress(self, cur, max):
        self.progress.emit('Saving fetched data', cur, max)

class FetchTask(TaskList):
    def __init__(self, fetch_event, fetch_since, fetch_photos, username, password, progress, batch):
        tasks = [LoginTask(username, password)]
        if fetch_event:
            tasks.append(ReportTask(fetch_event, fetch_since, progress))
        if fetch_photos != 'none':
            people = Person.all_with_photos(fetch_photos)
            tasks.append(PhotosTask(progress, people, batch))
        broken_registrations = Registration.by_category('')
        if broken_registrations:
            tasks.append(CategoryTask(progress, list(broken_registrations), batch))
        tasks.append(BatchFinishTask(progress, batch))
        TaskList.__init__(self, tasks)

class FetchPersonTask(TaskList):
    def __init__(self, person, username, password, progress, batch):
        tasks = [LoginTask(username, password)]
        # XXX: We should also fetch the person's record, but that would mean writing a parser for it
        tasks.append(PhotosTask(progress, [Person.get(id=person)], batch))
        TaskList.__init__(self, tasks)

class FetchWorker(QtCore.QObject):
    completed = QtCore.pyqtSignal(int)
    error = QtCore.pyqtSignal(str)
    progress = QtCore.pyqtSignal(str, int, int)
    
    def __init__(self):
        QtCore.QObject.__init__(self)

    @QtCore.pyqtSlot(int, QtCore.QDate, str, str, str)
    @catcherror
    def start_fetch(self, fetch_event, fetch_since, fetch_photos, username, password):
        self.batch = Batch()
        self.task = FetchTask(fetch_event, fetch_since, fetch_photos, username, password, self.progress, self.batch)
        self.task.task_finished.connect(self.batch.finish)
        self.task.task_exception.connect(self.handle_exception)
        self.batch.finished.connect(lambda: self.completed.emit(fetch_event))
        self.progress.emit('Logging in', 0, 0)

        self.task.start_task()

    @QtCore.pyqtSlot(int, QtCore.QDate, str, str, str)
    @catcherror
    def start_fetch_person(self, person, username, password):
        self.batch = Batch()
        self.task = FetchPersonTask(person, username, password, self.progress, self.batch)
        self.task.task_finished.connect(self.batch.finish)
        self.task.task_exception.connect(self.handle_exception)
        self.batch.finished.connect(lambda: self.completed.emit(0))
        self.progress.emit('Logging in', 0, 0)

        self.task.start_task()

    def handle_exception(self, e, msg):
        if isinstance(e, LoginError):
            self.error.emit(str(e))
        else:
            self.error.emit(msg)

class Fetcher(QtCore.QObject):
    sig_start_fetch = QtCore.pyqtSignal(int, QtCore.QDate, str, str, str)
    sig_start_fetch_person = QtCore.pyqtSignal(int, str, str)
    
    def __init__(self):
        super(QtCore.QObject, self).__init__()
        
        self.fetcher = FetchWorker()
        #self.fetcher.moveToThread(thread_registry.get('network'))

        self.sig_start_fetch.connect(self.fetcher.start_fetch)
        self.sig_start_fetch_person.connect(self.fetcher.start_fetch_person)

        # This is an interesting idiom: copy the bound signals into
        # this object, so clients can just connect to them and hear
        # directly from the fetcher
        self.completed = self.fetcher.completed
        self.error = self.fetcher.error
        self.progress = self.fetcher.progress
        
    def start_fetch(self, fetch_event, fetch_since, fetch_photos, username, password):
        self.sig_start_fetch.emit(fetch_event, fetch_since, fetch_photos, username, password)

    def start_fetch_person(self, person, username, password):
        self.sig_start_fetch_person.emit(person, username, password)
