from __future__ import division
import re
from PyQt4 import QtCore
from ef.lib import SignalGroup
from ef.db import Person, Photo, Registration, Batch, FetchedPhoto, FindPhotos
import traceback
from ef.nettask import NetFuncs
from ef.task import Task
from ef.login import LoginTask, LoginError
from ef.threads import thread_registry
from ef.image import PhotoImage
from PIL import Image
from StringIO import StringIO

def catcherror(func):
    def wrapped(self, *args, **kwargs):
        try:
            func(self, *args, **kwargs)
        except:
            self.error.emit(traceback.format_exc())
    return wrapped

class UploadTask(Task, NetFuncs):
    completed = QtCore.pyqtSignal(bool)
    error = QtCore.pyqtSignal(str)

    def __init__(self, id, minimum_change, batch):
        Task.__init__(self)
        NetFuncs.__init__(self)

        self.id = id
        self.batch = batch
        self.person = Person(id)
        self.person.updated.connect(self.person_updated)
        self.reply = None
        self.minimum_change = minimum_change
        self.skipped = False

        self.task_finished.connect(self.complete)
        self.task_exception.connect(self.handle_exception)

    def complete(self):
        self.completed.emit(not self.skipped)

    def handle_exception(self, e, msg):
        self.error.emit(msg)

    def person_updated(self, origin):
        if origin != 'bind':
            return
        if self.person.current_photo_id is None:
            self.error.emit('Person %s has no current photo' % self.person)
            return
        self.photo = Photo(self.person.current_photo_id)
        self.photo.updated.connect(self.photo_updated)

    def photo_updated(self, origin):
        if origin != 'bind':
            return

        # Got what we need from the database

        # Read in the image

        try:
            image = Image.open(str(self.photo.full_path())).convert('RGBA')
        except IOError, e:
            if e.errno == errno.ENOENT:
                # Just skip images that haven't been downloaded, we
                # couldn't possibly want to upload something we
                # haven't looked at
                self.completed.emit(False)
                return
            raise

        photoimage = PhotoImage(image)
        photoimage.set_rotation(self.photo.rotate)
        photoimage.set_brightness(self.photo.brightness)
        photoimage.set_contrast(self.photo.contrast)
        photoimage.set_gamma(self.photo.gamma)
        photoimage.set_crop_scale(self.photo.crop_scale)
        photoimage.set_crop_centre(self.photo.crop_centre_x, self.photo.crop_centre_y)
        photoimage.set_crop(True)

        orig_size = image.size[0] * image.size[1]
        image = photoimage.make_image()
        new_size = image.size[0] * image.size[1]

        size_change = new_size / orig_size
        if self.photo.url and self.photo.rotate == 0 and (100 * abs(1 - size_change)) < self.minimum_change:
            # This photo hasn't changed enough so we'll skip it
            self.skipped = True
            self.completed.emit(False)
            return

        # Start the process of uploading the edited image to eventsforce
        self.start_task(image)

    def extract_link_from_silly_button(self, button):
        m = re.match(r'document.location=\'(.*)\';', button['onclick'])
        if not m:
            self.error.emit("Could not parse javascript handler link %s", button['onclick'])
            return None
        return m.group(1)

    def prepare_file_upload(self, name, image):
        filename = '%d_%s.jpg' % (self.person.id, self.person.fullname)
        filename = re.sub(r'[ #?/:]', '_', filename)

        data = StringIO()
        image.save(data, 'jpeg')

        buffer = QtCore.QBuffer(self)
        buffer.setData(data.getvalue())
        buffer.open(QtCore.QIODevice.ReadOnly)

        # Make sure buffer sticks around while the network operation runs
        self.buffer = buffer
        
        return {'name': name,
                'filename': filename,
                'type': 'image/jpeg',
                'device': buffer,
                }

    def task(self, image):
        soup = yield self.get('https://www.eventsforce.net/libdems/backend/home/codEditMain.csp?codReadOnly=1&personID=%d&curPage=1' % self.id)

        links = soup.find_all('a', href=re.compile(r'^\.\./\.\./frontend/reg/initSession'))
        if not len(links):
            self.error.emit("Could not find any event links in page for %s" % self.person)
            return

        link = links[-1]['href']
        soup = yield self.get(link)

        edit_button = soup.find('input', type='button', value='Edit')
        link = self.extract_link_from_silly_button(edit_button)
        if link is None:
            return
        soup = yield self.get(link)

        while not re.match(r'Photo upload', soup.find_all('h1')[1].text.strip(), re.I):
            soup = yield self.submit_form(soup.form)

        remove_link = soup.find('a', text='remove', href=re.compile(r'javascript: removeFile'))
        m = re.search(r'removeFile\((\d+)\s*,\s*(\d+)\)', remove_link['href'])
        if not m:
            self.error.emit("Could not parse javascript remove function %s" % remove_link['href'])
            return
        temp_person_id = m.group(1)
        item_name_id = m.group(2)

        soup = yield self.submit_form(soup.form, {'deleteFile': item_name_id, 'uploadTempPersonID': temp_person_id, 'uploadItemNameID': item_name_id})

        upload_button = soup.find('input', type='button', value='Upload')
        m = re.search(r'SaveAndUpload\((\d+)\s*,\s*(\d+)\s*,\s*(\d+)\)', upload_button['onclick'])
        if not m:
            self.error.emit("Could not parse javascript handler link %s" % upload_button['onclick'])
            return

        temp_person_id = m.group(1)
        data_id = m.group(2)
        guest_number = m.group(3)

        soup = yield self.submit_form(soup.form, {'uploadFile': '1', 'uploadTempPersonID': temp_person_id, 'uploadGuestNumber': guest_number, 'uploadDataID': data_id})
        soup = yield self.submit_form(soup.form, {}, self.prepare_file_upload('FileStream', image))

        for script in soup.find_all('script'):
            m = re.match(r'^\s*window\.location=\'(.*)\';', script.text)
            if m:
                break
        link = m.group(1)

        if re.search(r'File could not be saved', link):
            # retry loop is being constructed here - can't reproduce reliably so one piece at a time
            soup = yield self.get(link)
            print link
            print soup
            soup = yield self.submit_form(soup.form)
            print soup
        
        if not re.search(r'uploadSuccess=1', link):
            self.error.emit("Upload failed, error link: %s" % link)
            return

        soup = yield self.get(link)

        ok_button = soup.find('input', type='button', value='OK')
        link = self.extract_link_from_silly_button(ok_button)

        soup = yield self.get(link)

        link = soup.find('a', href=re.compile(r'^/LIBDEMS/media/delegate_files/'))

        new_photo_url = str(self.current.resolve_url(link['href']).toString())

        while not re.search(r'Booking details', soup.find_all('h1')[1].text.strip(), re.I):
            soup = yield self.submit_form(soup.form)

        final_proceed_button = soup.find('input', type='button', onclick=re.compile(r'gotoReceipt'))
        link = self.extract_link_from_silly_button(final_proceed_button)
        soup = yield self.get(link)

        link = soup.find('a', text='CONFIRM')

        soup = yield self.get(link['href'])

        if not re.search(r'Booking confirmation', soup.find_all('h1')[1].text, re.I):
            self.error.emit('Final page after upload did not look right, did something bad happen?')

        new_opinion = None
        if self.photo.opinion == 'ok':
            new_opinion = 'ok'

        self.fetchedphoto = FetchedPhoto(self.id, new_photo_url, self.batch, opinion=new_opinion)
        self.fetchedphoto.run()

class UploadWorker(QtCore.QObject):
    completed = QtCore.pyqtSignal()
    error = QtCore.pyqtSignal(str)
    progress = QtCore.pyqtSignal(str, int, int)
    
    def __init__(self):
        super(QtCore.QObject, self).__init__()

        self.tasks = None
        self.reply = None

    @QtCore.pyqtSlot(dict, str, str)
    @catcherror
    def start_upload(self, ids, username, password):
        self.aborted = False
        self.ids = ids
        self.i = 0
        self.upload_count = 0
        self.tasks = {}
        self.batch = Batch()
        self.batch.finished.connect(self.completed)
        self.username = username
        self.password = password
        self.percent_filter = 0
        self.login_task = LoginTask(self.username, self.password)
        self.login_task.task_exception.connect(self.handle_login_exception)
        self.login_task.start_task()

        signals = [self.login_task.task_finished]

        if self.ids['mode'] == 'good' or self.ids['mode'] == 'percent':
            self.people = None
            if self.ids['mode'] == 'percent':
                self.percent_filter = self.ids['filter']

            self.find_photos = FindPhotos('good')
            signals.append(self.find_photos.results)
        else:
            self.people = self.ids['people']
            self.find_photos = None

        self.ready_group = SignalGroup(*signals)
        self.ready_group.fire.connect(self.ready)

        if self.find_photos is not None:
            self.find_photos.run()

        self.progress.emit('Logging in', 0, 0)

    def ready(self):
        if self.people is None:
            self.people = self.find_photos.result()
        self.next_task()

    def next_task(self):
        self.progress.emit('Uploading photos', self.i, len(self.people))

        if self.i >= len(self.people):
            self.batch.progress.connect(self.handle_commit_progress)
            self.batch.finish()
            return

        id = self.people[self.i]
        self.tasks[id] = UploadTask(id, self.percent_filter, self.batch)
        self.tasks[id].completed.connect(self.handle_task_complete)
        self.tasks[id].error.connect(self.handle_error)

    def handle_task_complete(self, uploaded):
        if uploaded:
            self.upload_count = self.upload_count + 1
        self.i = self.i + 1
        self.next_task()

    def handle_commit_progress(self, cur, max):
        self.progress.emit('Saving new photo URLs', cur, max)

    def handle_login_exception(self, e, msg):
        if isinstance(e, LoginError):
            self.handle_error(str(e))
        else:
            self.handle_error(m)

    def handle_error(self, err):
        if self.aborted:
            return
        self.aborted = True

        self.error.emit(err)
        if self.tasks is not None:
            for task in self.tasks.itervalues():
                task.abort()

class Uploader(QtCore.QObject):
    sig_start_upload = QtCore.pyqtSignal(dict, str, str)
    
    def __init__(self):
        super(QtCore.QObject, self).__init__()
        
        self.uploader = UploadWorker()
        #self.uploader.moveToThread(thread_registry.get('network'))

        self.sig_start_upload.connect(self.uploader.start_upload)

        self.completed = self.uploader.completed
        self.error = self.uploader.error
        self.progress = self.uploader.progress
        
    def start_upload(self, ids, username, password):
        self.sig_start_upload.emit(ids, username, password)
