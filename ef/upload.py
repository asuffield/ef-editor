from __future__ import division
import re
import sys
from PyQt4 import QtCore
from ef.lib import SignalGroup
from ef.db import Person, Photo, Registration, Batch, FetchedPhoto
import traceback
from ef.nettask import NetFuncs
from ef.task import Task
from ef.login import LoginTask, LoginError
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
    completed = QtCore.pyqtSignal(bool, bool)
    error = QtCore.pyqtSignal(str)
    progress = QtCore.pyqtSignal(int)

    def __init__(self, person, minimum_change, batch):
        Task.__init__(self)
        NetFuncs.__init__(self)

        self.batch = batch
        self.person = person
        self.photo = Photo.get(id=self.person.current_photo_id)
        self.reply = None
        self.minimum_change = minimum_change
        self.skipped = False
        self.aborted = False

        self.task_finished.connect(self.complete)
        self.task_exception.connect(self.handle_exception)

    def start(self):
        if self.person.current_photo_id is None:
            self.error.emit('Person %s has no current photo' % self.person)
            return

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

    def abort(self):
        self.aborted = True
        Task.abort(self)

    def complete(self):
        self.completed.emit(not self.skipped, self.aborted)

    def handle_exception(self, e, msg):
        self.error.emit(msg)

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
        soup = yield self.get('https://www.eventsforce.net/libdems/backend/home/codEditMain.csp?codReadOnly=1&personID=%d&curPage=1' % self.person.id)

        self.progress.emit(1)

        links = soup.find_all('a', href=re.compile(r'^\.\./\.\./frontend/reg/initSession'))
        if not len(links):
            self.error.emit("Could not find any event links in page for %s" % self.person)
            return

        link = links[-1]['href']
        soup = yield self.get(link)

        self.progress.emit(2)

        edit_button = soup.find('input', type='button', value='Edit')
        link = self.extract_link_from_silly_button(edit_button)
        if link is None:
            return
        soup = yield self.get(link)

        self.progress.emit(3)

        limit = 20
        while limit > 0 and not soup.find_all(['h1', 'h2', 'h3', 'h4'], text=re.compile(r'\s*Photo Upload\s*')):
            if not soup.form:
                self.error.emit("Could not find form on registration pages (while looking for photo upload)")
            soup = yield self.submit_form(soup.form)
            error = soup.find('script', text=re.compile(r".*alert\('Error", re.S|re.I))
            if error:
                m = re.search(r"alert\('(.*)'\);", error.text, re.S|re.I)
                if m:
                    msg = m.group(1)
                    msg = msg.replace(r'\r', '\r').replace(r'\n', '\n')
                    self.error.emit("Error from eventsforce: %s" % msg)
                else:
                    self.error.emit("Eventsforce generated an error, but it couldn't be recognised. Please try the web interface to see what's going on.")
            limit = limit - 1

        if limit == 0:
            #f = open('tmp.html', 'w')
            #f.write(str(soup))
            #f.close()
            self.error.emit("Failed to find photo upload on registration pages")

        self.progress.emit(4)

        remove_link = soup.find('a', text='remove', href=re.compile(r'javascript: removeFile'))
        if remove_link:
            m = re.search(r'removeFile\((\d+)\s*,\s*(\d+)\)', remove_link['href'])
            if not m:
                self.error.emit("Could not parse javascript remove function %s" % remove_link['href'])
                return
            temp_person_id = m.group(1)
            item_name_id = m.group(2)

            soup = yield self.submit_form(soup.form, {'deleteFile': item_name_id, 'uploadTempPersonID': temp_person_id, 'uploadItemNameID': item_name_id})

        self.progress.emit(5)

        upload_button = soup.find('input', type='button', value='Upload')
        if not upload_button:
            self.error.emit("Could not find image 'Upload' button in eventsforce page")
            return
        m = re.search(r'SaveAndUpload\((\d+)\s*,\s*(\d+)\s*,\s*(\d+)\)', upload_button['onclick'])
        if not m:
            self.error.emit("Could not parse javascript handler link %s" % upload_button['onclick'])
            return

        temp_person_id = m.group(1)
        data_id = m.group(2)
        guest_number = m.group(3)

        soup = yield self.submit_form(soup.form, {'uploadFile': '1', 'uploadTempPersonID': temp_person_id, 'uploadGuestNumber': guest_number, 'uploadDataID': data_id})
        self.progress.emit(6)
        soup = yield self.submit_form(soup.form, {}, self.prepare_file_upload('FileStream', image))
        self.progress.emit(7)

        for script in soup.find_all('script'):
            m = re.match(r'^\s*window\.location=\'(.*)\';', script.text)
            if m:
                break
        if not m:
            #print soup
            self.error.emit("Could not process photo upload (failed to find javascript refresh)")
        link = m.group(1)

        if re.search(r'File could not be saved', link):
            self.error.emit("Eventsforce broke while uploading file")
            return

        if not re.search(r'uploadSuccess=1', link):
            self.error.emit("Upload failed, error link: %s" % link)
            return

        self.progress.emit(8)

        soup = yield self.get(link)

        self.progress.emit(9)

        ok_button = soup.find('input', type='button', value='OK')
        link = self.extract_link_from_silly_button(ok_button)

        soup = yield self.get(link)

        self.progress.emit(10)

        link = soup.find('a', href=re.compile(r'^/LIBDEMS/media/delegate_files/'))
        if not link:
            self.error.emit("Could not find link to newly uploaded file")
            return

        href_url = QtCore.QUrl()
        href_url.setEncodedUrl(link['href'])
        new_photo_url = str(self.current.resolve_url(href_url).toEncoded())

        limit = 20
        while limit > 0 and not re.search(r'Booking details', soup.find_all('h1')[1].text.strip(), re.I):
            if not soup.form:
                self.error.emit("Could not find form on registration pages (while looking for final booking details page)")
            soup = yield self.submit_form(soup.form)
            limit = limit - 1

        if limit == 0:
            self.error.emit("Failed to reach end of registration pages after uploading photo")

        self.progress.emit(11)

        final_proceed_button = soup.find('input', type='button', onclick=re.compile(r'gotoReceipt'))
        if not final_proceed_button:
            self.error.emit("Could not find final SAVE button")
        link = self.extract_link_from_silly_button(final_proceed_button)
        soup = yield self.get(link)

        self.progress.emit(12)

        link = soup.find('a', text='CONFIRM')
        if link:
            soup = yield self.get(link['href'])

        self.progress.emit(13)

        if not re.search(r'Booking confirmation', soup.find_all('h1')[1].text, re.I):
            self.error.emit('Final page after upload did not look right, did something bad happen?')

        new_opinion = None
        if self.photo.opinion == 'ok':
            new_opinion = 'ok'

        self.fetchedphoto = FetchedPhoto(self.person, new_photo_url, self.batch, opinion=new_opinion, uploaded=True)

def person_should_upload(person):
    if person.current_photo_id is None:
        return False
    photo = Photo.get(id=person.current_photo_id)
    if photo.block_upload:
        return False

    if photo.width * photo.height == 0:
        return False

    space_used = (6 * photo.height) / (8 * photo.width)
    if space_used > 1:
        space_used = 1 / space_used

    if space_used < 0.95:
        return True

    if photo.crop_centre_x != 0.5 or photo.crop_centre_y != 0.5 or photo.crop_scale != 1:
        return True
    if photo.brightness != 0 or photo.contrast != 0 or photo.gamma != 1:
        return True
    if photo.rotate != 0:
        return True

    return False

class UploadWorker(QtCore.QObject):
    # XXX: this should be a task
    completed = QtCore.pyqtSignal()
    error = QtCore.pyqtSignal(str)
    progress = QtCore.pyqtSignal(str, int, int)

    task_progress_size = 14
    
    def __init__(self):
        super(QtCore.QObject, self).__init__()

        self.tasks = None
        self.reply = None

    @QtCore.pyqtSlot(dict, str, str)
    @catcherror
    def start_upload(self, people_filter, username, password):
        self.aborted = False
        self.people_filter = people_filter
        self.i = 0
        self.upload_count = 0
        self.tasks = {}
        self.batch = Batch()
        self.batch.finished.connect(self.completed)
        self.username = username
        self.password = password
        self.percent_filter = 0
        self.retry_limit = 3

        if self.people_filter['mode'] == 'good' or self.people_filter['mode'] == 'percent':
            self.people = None
            if self.people_filter['mode'] == 'percent':
                self.percent_filter = self.people_filter['filter']

            self.people = Person.all_with_photos('good')
        else:
            self.people = self.people_filter['people']

        self.people = filter(person_should_upload, self.people)

        #print self.people

        self.login_task = LoginTask(self.username, self.password)
        self.login_task.task_exception.connect(self.handle_login_exception)
        self.login_task.start_task()
        self.login_task.task_finished.connect(self.next_task)

        self.progress.emit('Logging in', 0, 0)

    def next_task(self):
        self.progress.emit('Uploading photos', self.i * self.task_progress_size, len(self.people) * self.task_progress_size)

        if self.i >= len(self.people):
            self.batch.progress.connect(self.handle_commit_progress)
            self.batch.finish()
            return

        try:
            person = self.people[self.i]
            self.progress.emit('Uploading %s' % person, self.i * self.task_progress_size, len(self.people) * self.task_progress_size)
            self.tasks[person.id] = UploadTask(person, self.percent_filter, self.batch)
            self.current_task = self.tasks[person.id]
            self.tasks[person.id].completed.connect(self.handle_task_complete)
            self.tasks[person.id].error.connect(self.handle_upload_error)
            self.tasks[person.id].progress.connect(self.handle_task_progress)
            self.tasks[person.id].start()
        except Exception:
            self.error.emit(traceback.format_exc())

    def handle_task_complete(self, uploaded, aborted):
        if not aborted:
            if uploaded:
                self.upload_count = self.upload_count + 1
            self.i = self.i + 1
            self.retry_limit = 3
        self.next_task()

    def handle_task_progress(self, i):
        person = self.people[self.i]
        self.progress.emit('Uploading %s' % person, self.i * self.task_progress_size + i, len(self.people) * self.task_progress_size)

    def handle_commit_progress(self, cur, max):
        self.progress.emit('Saving new photo URLs', cur, max)

    def handle_login_exception(self, e, msg):
        if isinstance(e, LoginError):
            self.handle_error(str(e))
        else:
            self.handle_error(m)

    def handle_upload_error(self, err):
        self.retry_limit = self.retry_limit - 1
        if self.retry_limit <= 0:
            self.handle_error(self, err)
        else:
            print "Upload of %s failed (retrying %d more times): %s" % (self.people[self.i], self.retry_limit, err)
            self.current_task.abort()

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
        
    def start_upload(self, people_filter, username, password):
        self.sig_start_upload.emit(people_filter, username, password)
