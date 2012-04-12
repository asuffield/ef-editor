from __future__ import division
import re
from PyQt4 import QtCore, QtGui, QtNetwork
from ef.lib import WorkerThread
from ef.db import Person, Photo, Registration, Batch, FetchedPhoto, FindPhotos
from ef.parser import EFDelegateParser
from bs4 import BeautifulSoup
import traceback
import time
from ef.netlib import qt_form_post, qt_page_get, qt_reply_charset, qt_readall_charset, qt_relative_url

def catcherror(func):
    def wrapped(self, *args, **kwargs):
        try:
            func(self, *args, **kwargs)
        except:
            self.error.emit(traceback.format_exc())
    return wrapped

class UploadTask(QtCore.QObject):
    completed = QtCore.pyqtSignal(bool)
    error = QtCore.pyqtSignal(str)

    def __init__(self, id, minimum_change, manager, batch):
        super(QtCore.QObject, self).__init__()

        self.id = id
        self.manager = manager
        self.batch = batch
        self.person = Person(id)
        self.person.updated.connect(self.person_updated)
        self.reply = None
        self.minimum_change = minimum_change

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

        # XXX: skip images that aren't downloaded? Yes, no cropping without checking
        reader = QtGui.QImageReader(self.photo.full_path())
        image = reader.read()

        if image.isNull():
            if reader.error() == QtGui.QImageReader.FileNotFoundError:
                # Just skip images that haven't been downloaded, we
                # couldn't possibly want to upload something we
                # haven't looked at
                self.completed.emit(False)
                return
            self.error.emit(reader.errorString())
            return

        # Now we need to scale the image, etc...

        orig_size = image.width() * image.height()

        image = image.transformed(QtGui.QTransform().rotate(self.photo.rotate))
        width = image.width()
        height = image.height()
        if (width/height) > (6/8):
            crop_height = float(height)
            crop_width = crop_height * 6/8
        else:
            crop_width = float(width)
            crop_height = crop_width * 8/6
        crop_width = crop_width * self.photo.crop_scale
        crop_height = crop_height * self.photo.crop_scale

        crop_centre_x = width * self.photo.crop_centre_x
        crop_centre_y = height * self.photo.crop_centre_y

        image = image.copy(crop_centre_x - crop_width/2, crop_centre_y - crop_height/2, crop_width, crop_height)

        new_size = image.width() * image.height()

        size_change = new_size / orig_size
        if (100 * abs(1 - size_change)) < self.minimum_change:
            # This photo hasn't changed enough so we'll skip it
            self.completed.emit(False)
            return

        # Stash the edited image for debugging
        #writer = QtGui.QImageWriter('out.jpeg', 'jpeg')
        #writer.setQuality(95)
        #writer.write(image)

        # Start the process of uploading the edited image to eventsforce
        self.coro = self.upload(image)
        self.setup_coro_signals(self.coro.next())

    def extract_link_from_silly_button(self, button):
        m = re.match(r'document.location=\'(.*)\';', button['onclick'])
        if not m:
            self.error.emit("Could not parse javascript handler link %s", button['onclick'])
            return None
        return m.group(1)

    def submit_form(self, form, user_fields={}, file=None):
        fields = {}
        action = form['action']

        for input in form.find_all('input'):
            if not input.has_key('name'):
                continue
            name = input['name']
            if input['type'] == 'image':
                fields['%s.x' % name] = '1'
                fields['%s.y' % name] = '1'
            elif input['type'] == 'button':
                continue
            elif input.has_key('value'):
                fields[name] = input['value']

        fields.update(user_fields)

        return qt_form_post(self.manager, qt_relative_url(self.reply, action), fields, file)

    def upload(self, image):
        soup = yield qt_page_get(self.manager,
                                 'https://www.eventsforce.net/libdems/backend/home/codEditMain.csp?codReadOnly=1&personID=%d&curPage=1' % self.id)

        links = soup.find_all('a', href=re.compile(r'^\.\./\.\./frontend/reg/initSession'))
        if not len(links):
            self.error.emit("Could not find any event links in page for %s" % self.person)
            return

        link = links[-1]['href']
        soup = yield qt_page_get(self.manager, qt_relative_url(self.reply, link))

        edit_button = soup.find('input', type='button', value='Edit')
        link = self.extract_link_from_silly_button(edit_button)
        if link is None:
            return
        soup = yield qt_page_get(self.manager, qt_relative_url(self.reply, link))

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

        filename = '%d_%s.jpg' % (self.person.id, self.person.fullname)
        filename = re.sub(r'[ #?/:]', '_', filename)

        buffer = QtCore.QBuffer(self)
        buffer.open(QtCore.QIODevice.ReadWrite)
        writer = QtGui.QImageWriter(buffer, 'jpeg')
        writer.setQuality(95)
        writer.write(image)
        buffer.seek(0)

        # Make sure buffer sticks around while the network operation runs
        self.buffer = buffer
        
        soup = yield self.submit_form(soup.form, {}, {'name': 'FileStream',
                                                      'filename': filename,
                                                      'type': 'image/jpeg',
                                                      'device': buffer,
                                                      })

        for script in soup.find_all('script'):
            m = re.match(r'^\s*window\.location=\'(.*)\';', script.text)
            if m:
                break
        link = m.group(1)
        if not re.search(r'uploadSuccess=1', link):
            self.error.emit("Upload failed, error link: %s" % link)
            return

        soup = yield qt_page_get(self.manager, qt_relative_url(self.reply, link))

        ok_button = soup.find('input', type='button', value='OK')
        link = self.extract_link_from_silly_button(ok_button)

        soup = yield qt_page_get(self.manager, qt_relative_url(self.reply, link))

        link = soup.find('a', href=re.compile(r'^/LIBDEMS/media/delegate_files/'))
        print 'New photo is at', link['href']

        while not re.search(r'Booking details', soup.find_all('h1')[1].text.strip(), re.I):
            soup = yield self.submit_form(soup.form)

        final_proceed_button = soup.find('input', type='button', onclick=re.compile(r'gotoReceipt'))
        link = self.extract_link_from_silly_button(final_proceed_button)
        soup = yield qt_page_get(self.manager, qt_relative_url(self.reply, link))

        link = soup.find('a', text='CONFIRM')

        soup = yield qt_page_get(self.manager, qt_relative_url(self.reply, link['href']))

        if not re.search(r'Booking confirmation', soup.find_all('h1')[1].text, re.I):
            self.error.emit('Final page after upload did not look right, did something bad happen?')

    def setup_coro_signals(self, reply):
        self.reply = reply
        reply.finished.connect(self.handle_finished)

    def handle_finished(self):
        if self.reply.error() != QtNetwork.QNetworkReply.NoError:
            self.error.emit(self.reply.errorString())
            return

        self.reply.finished.disconnect(self.handle_finished)

        redirect = self.reply.attribute(QtNetwork.QNetworkRequest.RedirectionTargetAttribute)
        if redirect.isValid():
            url = qt_relative_url(self.reply, redirect.toString())
            reply = qt_page_get(self.manager, url)
            self.setup_coro_signals(reply)
            return

        charset = qt_reply_charset(self.reply)
        soup = BeautifulSoup(qt_readall_charset(self.reply, charset))

        try:
            reply = self.coro.send(soup)
            self.setup_coro_signals(reply)
        except StopIteration:
            self.completed.emit(True)
        except:
            self.error.emit(traceback.format_exc())

    def abort(self):
        if self.reply is not None and not self.reply.isFinished():
            self.reply.finished.disconnect(self.handle_finished)
            self.reply.abort()

class UploadWorker(QtCore.QObject):
    completed = QtCore.pyqtSignal()
    error = QtCore.pyqtSignal(str)
    progress = QtCore.pyqtSignal(str, int, int)
    
    def __init__(self):
        super(QtCore.QObject, self).__init__()

        self.manager = None
        self.tasks = None
        self.reply = None

    @QtCore.pyqtSlot(dict, str, str)
    @catcherror
    def start_upload(self, ids, username, password):
        if self.manager is None:
            self.manager = QtNetwork.QNetworkAccessManager()
            #self.manager.setProxy(QtNetwork.QNetworkProxy(QtNetwork.QNetworkProxy.HttpProxy, '127.0.0.1', 8080))

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

        if self.ids['mode'] == 'good' or self.ids['mode'] == 'percent':
            self.find_photos = FindPhotos('good')
            self.find_photos.results.connect(self.photos_ready)
            self.find_photos.run()
            if self.ids['mode'] == 'percent':
                self.percent_filter = self.ids['filter']
        else:
            self.people = self.ids['people']
            self.login()

    def photos_ready(self, people):
        self.people = people
        print people
        self.login()

    def login(self):
        self.progress.emit('Logging in', 0, 0)
        reply = qt_page_get(self.manager, 'https://www.eventsforce.net/libdems/backend/home/login.csp')
        reply.finished.connect(self.got_login_page)
        self.reply = reply

    def got_login_page(self):
        if self.reply.error() != QtNetwork.QNetworkReply.NoError:
            self.error.emit(self.reply.errorString())
            return

        self.reply.finished.disconnect(self.got_login_page)
        charset = qt_reply_charset(self.reply)
        soup = BeautifulSoup(qt_readall_charset(self.reply, charset))

        fields = {}
        for input in soup.form.find_all('input'):
            name = input['name']
            if input['type'] == 'image':
                fields['%s.x' % name] = '1'
                fields['%s.y' % name] = '1'
            else:
                fields[name] = input['value']

        fields['txtUsername'] = self.username
        fields['txtPassword'] = self.password
        
        reply = qt_form_post(self.manager, qt_relative_url(self.reply, soup.form['action']), fields)
        reply.finished.connect(self.login_finished)
        self.reply = reply

    def login_finished(self):
        if self.reply.error() != QtNetwork.QNetworkReply.NoError:
            self.error.emit(self.reply.errorString())
            return

        self.reply.finished.disconnect(self.login_finished)

        redirect = self.reply.attribute(QtNetwork.QNetworkRequest.RedirectionTargetAttribute)
        if redirect.isValid():
            url = qt_relative_url(self.reply, redirect.toString())
            self.reply = qt_page_get(self.manager, url)
            self.reply.finished.connect(self.login_finished)
            return
        
        self.next_task()

    def next_task(self):
        self.progress.emit('Uploading photos', self.i, len(self.people))

        if self.i >= len(self.people):
            self.batch.progress.connect(self.handle_commit_progress)
            self.batch.finish()
            return

        id = self.people[self.i]
        self.tasks[id] = UploadTask(id, self.percent_filter, self.manager, self.batch)
        self.tasks[id].completed.connect(self.handle_task_complete)
        self.tasks[id].error.connect(self.handle_error)

    def handle_task_complete(self, uploaded):
        if uploaded:
            self.upload_count = self.upload_count + 1
        self.i = self.i + 1
        self.next_task()

    def handle_commit_progress(self, cur, max):
        self.progress.emit('Saving new photo URLs', cur, max)

    def handle_error(self, err):
        if self.aborted:
            return
        self.aborted = True

        if self.reply is not None and not self.reply.isFinished():
            self.reply.abort()

        self.error.emit(err)
        if self.tasks is not None:
            for task in self.tasks.itervalues():
                task.abort()

class Uploader(QtCore.QObject):
    sig_start_upload = QtCore.pyqtSignal(dict, str, str)
    
    def __init__(self):
        super(QtCore.QObject, self).__init__()
        
        self.worker = WorkerThread()
        self.uploader = UploadWorker()
        self.uploader.moveToThread(self.worker)

        self.sig_start_upload.connect(self.uploader.start_upload)

        self.completed = self.uploader.completed
        self.error = self.uploader.error
        self.progress = self.uploader.progress
        
        self.worker.start()

    def start_upload(self, ids, username, password):
        self.sig_start_upload.emit(ids, username, password)
