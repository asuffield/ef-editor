#!/usr/bin/python

from __future__ import division

import sys
import os
import traceback
import time
import tempfile
import shutil
from PyQt4 import QtCore, QtGui
from ef.ui.editor import Ui_ImageEdit
from ef.ui.upload_wizard import Ui_UploadPeopleWizard
from ef.db import Person, Photo, setup_session, FindRegistrations, FindCategories, FindPoliceStatus, Event, FetchPhotoHistory, Batch, FetchedPhoto, dbmanager, stash_photo
from ef.photocache import ThumbnailCache, PhotoImageCache
from ef.photodownload import PhotoDownloader
from ef.fetch import Fetcher
from ef.fetchreports import ReportsFetcher
from ef.fetchwizard import FetchWizard
from ef.upload import Uploader
from ef.threads import thread_registry
from ef.netlib import start_network_manager
from ef.filtercontrol import FilterProxyModel
from collections import deque
from datetime import datetime
from PIL import Image

class ImageListItem(QtGui.QStandardItem):
    def __init__(self, downloader, photo_cache, person_id):
        QtGui.QStandardItem.__init__(self)

        self.person = Person(person_id)
        self.person.updated.connect(self.person_updated)
        self.photo = None
        self.photo_cache = photo_cache
        self.downloader = downloader
        self.photo_load_retries = 0
        self.loading = False
        self.db_loaded = False
        self.registrations = []
        self.findregistrations = FindRegistrations(person_id)
        self.findregistrations.finished.connect(self.registrations_updated)
        self.findregistrations.run()

        self.size_hint = QtCore.QSize(80, 160)

        self.setFlags(QtCore.Qt.ItemIsEnabled | QtCore.Qt.ItemIsSelectable)

    def data(self, role):
        if role == QtCore.Qt.DecorationRole:
            return self.get_photo()

        if role == QtCore.Qt.DisplayRole:
            return self.person.fullname

        if role == QtCore.Qt.ToolTipRole:
            return unicode(self.person)

        if role == QtCore.Qt.StatusTipRole:
            return unicode(self.person)

        if role == QtCore.Qt.SizeHintRole:
            return self.size_hint

        if role == QtCore.Qt.UserRole:
            return self.person.id

        if role == QtCore.Qt.UserRole+1:
            return u"%s %s" % (self.person.lastname, self.person.firstname)

        if role == QtCore.Qt.UserRole+2:
            if self.photo is None:
                return None
            return self.photo.opinion

        if role == QtCore.Qt.UserRole+3:
            return self.person.police_status

        if role == QtCore.Qt.UserRole+4:
            return self.registrations

        if role == QtCore.Qt.UserRole+5:
            if self.photo is None:
                return None
            return (self.photo.width, self.photo.height)

        if role == QtCore.Qt.UserRole+6:
            if self.photo is None:
                return None
            return self.photo.full_path()

        if role == QtCore.Qt.UserRole+7:
            if self.photo is None:
                return None
            return self.photo.id

        if role == QtCore.Qt.UserRole+8:
            return self.db_loaded

        return None

    def person_updated(self, origin):
        if origin == 'CropFrame':
            return
        if self.person.current_photo_id is None:
            return
        if self.photo is not None and self.photo.id == self.person.current_photo_id:
            return
        if self.photo is not None:
            self.photo.updated.disconnect(self.photo_updated)
        self.photo = Photo(self.person.current_photo_id)
        self.photo.updated.connect(self.photo_updated)
        self.photo_load_retries = 0
        self.emitDataChanged()

    def photo_updated(self, origin):
        if origin == 'CropFrame':
            return
        self.loading = False
        self.db_loaded = True
        self.photo_load_retries = 0
        # Populate the disk cache in the background, but don't load into the application just yet
        self.downloader.download_photo(self.photo.id, self.photo.url, self.photo.full_path(), background=True)
        self.emitDataChanged()

    def registrations_updated(self):
        self.registrations = self.findregistrations.result()
        self.emitDataChanged()

    def handle_photo_ready(self, id, image):
        self.loading = False
        self.emitDataChanged()

    def handle_photo_fail(self, id, error):
        self.loading = False
        self.photo_load_retries = self.photo_load_retries + 1
        self.emitDataChanged()

    def get_photo(self):
        if self.photo_load_retries > 3 or self.photo is None or self.loading:
            return None

        pixmap = self.photo_cache.peek_image(self.photo.id)
        if pixmap is not None:
            return pixmap

        # When this finishes, we'll just update the item in the list
        # so this function gets called again, and hopefully the pixmap
        # we wanted will now be in the cache

        # (There's a bug here in the pathological case where we cycle
        # through loading more images than the cache can fit - screw it)
        
        self.photo_cache.load_image(self.photo.id,
                                    self.photo.full_path(),
                                    self.photo.url,
                                    ready_cb=self.handle_photo_ready,
                                    fail_cb=self.handle_photo_fail)
        self.loading = True
        return None

class CropFrame(QtGui.QGraphicsRectItem):
    def __init__(self, main_pixmap, output_updated):
        super(QtGui.QGraphicsRectItem, self).__init__(0, 0, 0, 0, main_pixmap)

        self.hide()

        self.main_pixmap = main_pixmap
        self.output_updated = output_updated
        self.photo = None

        self.scale_factor = 1.0
        self.image_width = self.image_height = self.scaled_width = self.scaled_height = 0

        self.crop_pen = QtGui.QPen()
        self.crop_pen.setWidth(3)
        self.crop_pen.setStyle(QtCore.Qt.DashLine)
        self.setPen(self.crop_pen)
        self.setFlag(QtGui.QGraphicsItem.ItemIsMovable)
        self.setFlag(QtGui.QGraphicsItem.ItemSendsGeometryChanges)

    def setup_new_image(self, width, height, photo):
        self.image_width = width
        self.image_height = height
        self.photo = None
        
        if (width/height) > (6/8):
            self.crop_height = float(height)
            self.crop_width = self.crop_height * 6/8
        else:
            self.crop_width = float(width)
            self.crop_height = self.crop_width * 8/6

        self.setPos(width * photo.crop_centre_x, height * photo.crop_centre_y)

        self.crop_pen.setWidth(self.crop_width/100)
        self.setPen(self.crop_pen)

        self.scale_factor = photo.crop_scale

        self.update_rect()
        self.show()
        
        self.photo = photo

    def update_db(self):
        if self.photo is None:
            return
        
        x = self.x()
        y = self.y()

        self.photo.update_crop(x / self.image_width, y / self.image_height, self.scale_factor, 'CropFrame')

    def update_rect(self):
        self.scaled_width = self.crop_width*self.scale_factor
        self.scaled_height = self.crop_height*self.scale_factor
        self.setRect(-self.scaled_width/2, -self.scaled_height/2, self.scaled_width, self.scaled_height)
        self.output_updated.emit()

    def itemChange(self, change, value):
        if change == QtGui.QGraphicsItem.ItemPositionChange:
            pos = value.toPointF()

            xpos = pos.x()
            xoffset = self.scaled_width/2
            x1 = xpos - xoffset
            x2 = xpos + xoffset
            if self.scaled_width > self.image_width:
                if x1 > 0:
                    xpos = xoffset
                if x2 < self.image_width:
                    xpos = self.image_width - xoffset
            else:
                if x1 < 0:
                    xpos = xoffset
                if x2 > self.image_width:
                    xpos = self.image_width - xoffset

            ypos = pos.y()
            yoffset = self.scaled_height/2
            y1 = ypos - yoffset
            y2 = ypos + yoffset
            if self.scaled_height > self.image_height:
                if y1 > 0:
                    ypos = yoffset
                if y2 < self.image_height:
                    ypos = self.image_height - yoffset
            else:
                if y1 < 0:
                    ypos = yoffset
                if y2 > self.image_height:
                    ypos = self.image_height - yoffset
            
            return QtCore.QPointF(xpos, ypos)
        
        if change == QtGui.QGraphicsItem.ItemPositionHasChanged:
            self.update_db()
            self.output_updated.emit()
            
        return super(QtGui.QGraphicsRectItem, self).itemChange(change, value)

    def handle_wheel(self, delta):
        self.scale_factor += -delta / 2000.0
        self.update_db()
        self.update_rect()

    def cropping_rect(self):
        return self.main_pixmap.mapRectFromItem(self, self.rect()).toRect()

class EditPixmap(QtGui.QGraphicsPixmapItem):
    def __init__(self, wheel_event):
        super(QtGui.QGraphicsPixmapItem, self).__init__()
        self.wheel_event = wheel_event

    def wheelEvent(self, event):
        self.wheel_event.emit(event.delta())

class UploadPeopleWizard(QtGui.QWizard, Ui_UploadPeopleWizard):
    def __init__(self, parent=None):
        super(QtGui.QWizard, self).__init__(parent)
        self.setupUi(self)

        self.setButtonText(QtGui.QWizard.FinishButton, 'Start upload')

class ImageEdit(QtGui.QMainWindow, Ui_ImageEdit):
    output_updated = QtCore.pyqtSignal()
    wheel_event = QtCore.pyqtSignal(int)

    def __init__(self, parent=None):
        super(QtGui.QWidget, self).__init__(parent)
        self.setupUi(self)

        self.settings = QtCore.QSettings()

        self.history_make_current.setIcon(self.style().standardIcon(QtGui.QStyle.SP_ArrowRight))
        self.back.setIcon(self.style().standardIcon(QtGui.QStyle.SP_ArrowBack))
        self.forwards.setIcon(self.style().standardIcon(QtGui.QStyle.SP_ArrowForward))
        self.back.setDisabled(True)
        self.forwards.setDisabled(True)

        self.photodownloader = PhotoDownloader()
        self.list_photo_cache = ThumbnailCache(self.photodownloader, 100)
        self.main_photo_cache = PhotoImageCache(self.photodownloader, 10)
        self.fetcher = Fetcher()
        self.reportsfetcher = ReportsFetcher()
        self.uploader = Uploader()
        self.current_person = None
        self.current_photo = None
        self.current_image = None
        self.loading_now = False
        self.photo_load_failed = False

        self.history_back = deque()
        self.history_forwards = deque()
        self.suppress_history = False

        self.upload_wizard = UploadPeopleWizard()

        self.image_list_items = {}

        self.edit_scene = QtGui.QGraphicsScene(self)
        self.main_image.setScene(self.edit_scene)

        self.main_pixmap = EditPixmap(self.wheel_event)
        self.edit_scene.addItem(self.main_pixmap)

        self.crop_frame = CropFrame(self.main_pixmap, self.output_updated)

        self.main_pixmap.setZValue(1)
        self.crop_frame.setZValue(2)

        self.events = {}
        self.event_load_handlers = {}

        self.person_model = QtGui.QStandardItemModel(self)
        self.person_model.setColumnCount(1)

        self.person_model_proxy = FilterProxyModel()
        self.person_model_proxy.setDynamicSortFilter(True)
        self.person_model_proxy.setSourceModel(self.person_model)
        self.person_model_proxy.setSortCaseSensitivity(False)
        self.person_model_proxy.setSortRole(QtCore.Qt.UserRole+1)
        self.person_model_proxy.sort(0)

        self.person_model_proxy.rowsInserted.connect(self.handle_filter_count)
        self.person_model_proxy.rowsRemoved.connect(self.handle_filter_count)
        self.person_model_proxy.modelReset.connect(self.handle_filter_count)

        self.filter_opinion.currentIndexChanged[str].connect(self.person_model_proxy.set_opinion)
        self.filter_event.currentIndexChanged[int].connect(self.handle_filter_event_changed)
        self.filter_category.currentIndexChanged[str].connect(self.person_model_proxy.set_category)
        self.filter_police.currentIndexChanged[str].connect(self.person_model_proxy.set_police_status)
        self.filter_by_size.stateChanged.connect(self.person_model_proxy.set_only_bad_sizes)

        self.findcategories = FindCategories()
        self.findcategories.finished.connect(self.handle_categories)
        self.findcategories.run()

        self.findpolicestatus = FindPoliceStatus()
        self.findpolicestatus.finished.connect(self.handle_policestatus)
        self.findpolicestatus.run()

        self.history_model = QtGui.QStandardItemModel(self)
        self.history_model.setColumnCount(1)

        self.history_list.setModel(self.history_model)
        self.history_items = {}
        self.fetchphotohistory = FetchPhotoHistory()
        self.fetchphotohistory.ready.connect(self.handle_photohistory)
        self.history_make_current.clicked.connect(self.handle_historymakecurrent)

        dbmanager.created.connect(self.handle_db_created)
        dbmanager.exception.connect(self.handle_db_exception)
        dbmanager.existing_done.connect(self.handle_db_existing_done)
        Person.signal_existing_created()
        Event.signal_existing_created()

        self.output_updated.connect(self.handle_crop)
        self.wheel_event.connect(self.crop_frame.handle_wheel)
        self.opinion_ok.clicked.connect(self.handle_opinion_ok)
        self.opinion_bad.clicked.connect(self.handle_opinion_bad)
        self.opinion_unsure.clicked.connect(self.handle_opinion_unsure)
        self.back.clicked.connect(self.handle_back)
        self.forwards.clicked.connect(self.handle_forwards)
        self.search.clicked.connect(self.handle_search)
        self.search_for.returnPressed.connect(self.handle_search)

        self.action_fetch.triggered.connect(self.handle_fetch_wizard)

        self.fetcher.completed.connect(self.handle_fetch_completed)
        self.fetcher.error.connect(self.handle_fetch_error)
        self.fetcher.progress.connect(self.handle_fetch_progress)

        self.reportsfetcher.error.connect(self.handle_reportsfetch_error)

        self.action_upload.triggered.connect(self.handle_upload_wizard)
        self.upload_wizard.accepted.connect(self.handle_upload)
        self.upload_wizard.rejected.connect(self.handle_upload_rejected)

        self.uploader.completed.connect(self.handle_upload_completed)
        self.uploader.error.connect(self.handle_upload_error)
        self.uploader.progress.connect(self.handle_upload_progress)

        self.rotate.valueChanged.connect(self.handle_rotate)
        self.rotate_0.clicked.connect(lambda: self.rotate.setValue(0))
        self.rotate_l90.clicked.connect(lambda: self.rotate.setValue(-90))
        self.rotate_l180.clicked.connect(lambda: self.rotate.setValue(-180))
        self.rotate_r90.clicked.connect(lambda: self.rotate.setValue(90))
        self.rotate_r180.clicked.connect(lambda: self.rotate.setValue(180))
        self.brightness_slider.valueChanged.connect(self.handle_brightness)
        self.contrast_slider.valueChanged.connect(self.handle_contrast)

        self.gamma_slider.valueChanged.connect(lambda v: self.gamma_spin.setValue(v/10))
        self.gamma_spin.valueChanged.connect(lambda v: self.gamma_slider.setValue(v*10))
        self.gamma_slider.valueChanged.connect(self.handle_gamma)

        self.action_openeventsforce.triggered.connect(self.handle_openeventsforce)
        self.openeventsforce.clicked.connect(self.handle_openeventsforce)
        self.action_reloadphoto.triggered.connect(self.handle_reloadphoto)
        self.action_editimage.triggered.connect(self.handle_editimage)
        self.editimage.clicked.connect(self.handle_editimage)
        self.action_importphoto.triggered.connect(self.handle_import)

        self.openimage = QtGui.QFileDialog(self, 'Import image')
        self.openimage.setFileMode(QtGui.QFileDialog.ExistingFile)
        self.openimage.setNameFilter('*.jpg *.jpeg')
        self.openimage.restoreState(self.settings.value('openimage-state', '').toByteArray())

        self.action_chooseeditor.triggered.connect(self.handle_chooseeditor)

        self.chooseeditor = QtGui.QFileDialog(self, 'Choose image editor')
        self.chooseeditor.setFileMode(QtGui.QFileDialog.ExistingFile)
        if os.name == 'nt':
            self.chooseeditor.setNameFilter('*.exe')
        self.chooseeditor.restoreState(self.settings.value('chooseeditor-state', '').toByteArray())

        self.image_editor = self.settings.value('image-editor', '').toString()

        self.status_expiry_timer = QtCore.QTimer(self)
        self.status_expiry_timer.setInterval(5000)
        self.status_expiry_timer.timeout.connect(self.status_idle)

        self.status_task = ''
        self.status_started = None
        self.status_timer = QtCore.QTimer(self)
        self.status_timer.setInterval(500)
        self.status_timer.timeout.connect(self.status_timer_update)
        self.status_is_idle = True

        self.draw_timer = QtCore.QTimer(self)
        self.draw_timer.setInterval(50)
        self.draw_timer.timeout.connect(self.handle_draw)
        self.draw_timer.start()
        self.image_draw_needed = False

        self.photodownloader.queue_size.connect(self.status_downloader)

        self.procs = {}

        self.status_idle()

    def status_idle(self):
        self.progress.reset()
        self.progress.hide()
        self.status_is_idle = True
        self.status_downloader()

    def status_downloader(self):
        if self.status_is_idle:
            downloads = self.photodownloader.get_queue_size()
            if downloads > 0:
                self.status.setText('Downloading %d images' % downloads)
            else:
                self.status.setText('Idle')

    def status_start(self, task, max):
        self.status_expiry_timer.stop()
        self.progress.setRange(0, max)
        self.progress.show()
        self.status_is_idle = False

        if max == 0:
            if self.status_started is None:
                self.status_started = datetime.now()
                self.status_timer.start()
            self.status_task = task
            self.status_timer_update()
        else:
            self.status_timer.stop()
            self.status_started = None
            self.status.setText(task)

    def status_elapsed_str(self):
        td = datetime.now() - self.status_started
        minutes,seconds = divmod(td.seconds, 60)
        return "%d:%02d" % (minutes,seconds)

    def status_timer_update(self):
        if self.status_started is not None:
            self.status.setText("%s (%s)" % (self.status_task, self.status_elapsed_str())) 

    def status_finished(self):
        if self.status_started is not None:
            self.status_timer.stop()
            self.status.setText("%s (finished in %s)" % (self.status_task, self.status_elapsed_str())) 
            self.progress.setRange(0, 1)
            self.progress.setValue(1)
            self.status_started = None
        else:
            self.progress.setValue(self.progress.maximum())

        timer = QtCore.QTimer(self)
        self.status_expiry_timer.stop()
        self.status_expiry_timer.setSingleShot(True)
        self.status_expiry_timer.start()

    def clear_image(self):
        self.current_photo = None
        self.current_image = None
        self.image_draw_needed = False
        self.crop_frame.hide()
        self.main_pixmap.hide()
        self.preview_image.setPixmap(QtGui.QPixmap())
        self.person_name.setText(u'')
        self.upload_wizard.upload_photos_thisname.setText('')

    def foreach_item(self, f):
        for item in self.image_list_items.itervalues():
            f(item)

    def handle_search(self):
        query = str(self.search_for.text()).strip()

        # If the search box is empty, we want to show all items
        if len(query) == 0:
            self.person_model_proxy.id = None
            self.person_model_proxy.name = ''
            self.person_model_proxy.invalidateFilter()
            self.person_model_proxy.sort(0)
            return

        # If the query is just a number, treat it as a person ID query
        try:
            id = int(query)
        except ValueError:
            id = None

        if id is not None:
            self.person_model_proxy.id = id
        else:
            self.person_model_proxy.id = None
            self.person_model_proxy.name = query
        self.person_model_proxy.invalidateFilter()
        self.person_model_proxy.sort(0)

    def item_from_index(self, index):
        if index.isValid():
            index = self.person_model_proxy.mapToSource(index)
        if index.isValid():
            return self.person_model.itemFromIndex(index)
        else:
            return None

    def handle_select(self, current, previous):
        current = self.item_from_index(current)
        previous = self.item_from_index(previous)

        if current is None:
            return

        self.photo_load_failed = False
        person_id = current.data(QtCore.Qt.UserRole)
        self.load_person(person_id)

    def handle_model_item_changed(self, item):
        changed_index = self.person_model_proxy.mapFromSource(item.index())
        current_index = self.person_list.selectionModel().currentIndex()

        # If this item is the currently selected item...
        if changed_index.isValid() and current_index.isValid() and changed_index == current_index:
            # ...fish out the relevant data...
            person_id = item.data(QtCore.Qt.UserRole)

            p = self.image_list_items[person_id].person
            photo = self.image_list_items[person_id].photo

            # ...and see if it's the same thing we've already loaded
            if (self.current_person is not None and self.current_photo is not None
                and p.id == self.current_person.id and photo.id == self.current_photo.id):
                # If it's the same, just redraw the image to pick up photo control changes
                self.image_draw_needed = True
            else:
                # For anything else just reload the person
                self.load_person(person_id)

    """Load this person's photo into the editor"""
    def load_person(self, id, refresh=False):
        if self.photo_load_failed:
            return

        p = self.image_list_items[id].person
        photo = self.image_list_items[id].photo

        # Suppress history updates when changing the item with
        # back/forwards, because they have special handling
        if not self.suppress_history and self.current_photo is not None and self.current_person.id != id:
            self.history_back.append(self.current_person.id)
            self.back.setDisabled(False)
            self.history_forwards.clear()
            self.forwards.setDisabled(True)
            while len(self.history_back) > 10:
                self.history_back.popleft()

        self.clear_image()
        self.history_model.clear()
        self.current_person = p

        self.info_person_id.setText('%d' % self.current_person.id)
        self.info_fullname.setText(self.current_person.fullname)
        self.info_title.setText(self.current_person.title)
        self.info_firstname.setText(self.current_person.firstname)
        self.info_lastname.setText(self.current_person.lastname)
        self.info_police_status.setText(self.current_person.police_status)
        self.info_person_fetched_at.setText(time.ctime(self.current_person.last_checked_at))

        if photo is not None and photo.full_path() is not None:
            self.current_photo = photo
            self.info_photo_filename.setText(self.current_photo.url_filename())
            self.info_photo_fetched_at.setText(time.ctime(self.current_photo.date_fetched))
            self.person_name.setText(u'Loading %s...' % p)
            self.upload_wizard.upload_photos_thisname.setText(str(p))

            self.main_photo_cache.load_image(photo.id, photo.full_path(), photo.url,
                                             ready_cb=self.handle_photo_ready,
                                             fail_cb=self.handle_photo_fail,
                                             urgent=True, refresh=refresh,
                                             )
        else:
            self.person_name.setText(unicode(self.current_person))

        self.fetchphotohistory.run(p.id)

    def handle_photohistory(self, person_id):
        if not self.current_person or self.current_person.id != person_id:
            return
        photos = self.fetchphotohistory.get_photos(person_id)
        if photos is None:
            return
        self.history_model.clear()
        self.history_items = {}
        for photo in sorted(photos, key=lambda photo: photo.date_fetched, reverse=True):
            item = QtGui.QStandardItem()
            msg = "Fetched at %s" % time.ctime(photo.date_fetched)
            if self.current_person.current_photo_id == photo.id:
                msg = msg + "\nCurrent photo"
            else:
                msg = msg + "\n%s" % photo.opinion
            item.setText(msg)
            item.setTextAlignment(QtCore.Qt.AlignCenter)
            item.setData(photo.id)
            self.history_items[photo.id] = item
            self.history_model.appendRow(item)
            self.list_photo_cache.load_image(photo.id, photo.full_path(), photo.url,
                                             ready_cb=self.handle_history_photo_ready)

    def handle_history_photo_ready(self, photo_id, pixmap):
        item = self.history_items.get(photo_id, None)
        if item is not None:
            item.setData(pixmap, QtCore.Qt.DecorationRole)

    def handle_historymakecurrent(self):
        if not self.current_person:
            return
        item = self.history_model.itemFromIndex(self.history_list.currentIndex())
        if item is None:
            return
        photo_id = item.data().toPyObject()
        if photo_id != self.current_person.current_photo_id:
            self.current_person.update_current_photo(photo_id)

    def handle_reloadphoto(self):
        if self.current_person is not None:
            self.load_person(self.current_person.id, refresh=True)        

    """Select this id in person_list"""
    def select_person(self, id):
        index = self.image_list_items[id].index()
        if not index.isValid():
            return False
        index = self.person_model_proxy.mapFromSource(index)
        if not index.isValid():
            return False
        self.person_list.selectionModel().setCurrentIndex(index, QtGui.QItemSelectionModel.ClearAndSelect)
        return True

    """Get us to this id, in whatever way makes sense"""
    def jump_to_person(self, id):
        if not self.select_person(id):
            self.load_person(id)

    def handle_back(self):
        if self.current_person is not None:
            self.history_forwards.append(self.current_person.id)
            self.forwards.setDisabled(False)
        if len(self.history_back) > 0:
            id = self.history_back.pop()
            self.suppress_history = True
            self.jump_to_person(id)
            self.suppress_history = False
        if len(self.history_back) == 0:
            self.back.setDisabled(True)

    def handle_forwards(self):
        if self.current_person is not None:
            self.history_back.append(self.current_person.id)
            self.back.setDisabled(False)
        if len(self.history_forwards) > 0:
            id = self.history_forwards.pop()
            self.suppress_history = True
            self.jump_to_person(id)
            self.suppress_history = False
        if len(self.history_forwards) == 0:
            self.forwards.setDisabled(True)

    def handle_photo_fail(self, id, error):
        if self.current_photo is not None and self.current_photo.id == id:
            text = u'Failed to load %s: %s' % (self.current_person, error)
            self.clear_image()
            self.person_name.setText(text)
            self.photo_load_failed = True

    def handle_photo_ready(self, id, image):        
        if self.current_photo is None or self.current_photo.id != id:
            return

        angle = self.current_photo.rotate
        brightness = self.current_photo.brightness
        contrast = self.current_photo.contrast
        gamma = self.current_photo.gamma

        self.rotate.setValue(angle)
        self.brightness_slider.setValue(brightness*127)
        self.contrast_slider.setValue(contrast*127)
        self.gamma_slider.setValue(gamma*10)

        image.set_rotation(angle)
        image.set_brightness(brightness)
        image.set_contrast(contrast)
        image.set_gamma(gamma)

        self.current_image = image
        self.image_draw_needed = True

    def handle_draw(self):
        if self.image_draw_needed and self.current_image is not None:
            self.setup_photo()

    def setup_photo(self):
        # Suppress re-entrant noise, because loading a photo emits
        # signals like editing would
        self.loading_now = True

        image = self.current_image.make_qimage()
        pixmap = QtGui.QPixmap.fromImage(image.copy())
        self.main_pixmap.setPixmap(pixmap)

        width = pixmap.width()
        height = pixmap.height()
        self.main_pixmap.setTransformOriginPoint(width/2, height/2)
        self.edit_scene.setSceneRect(0, 0, width, height)
        self.crop_frame.setup_new_image(width, height, self.current_photo)
        self.main_pixmap.show()
        self.person_name.setText(unicode(self.current_person))
        self.main_image.fitInView(self.main_pixmap, QtCore.Qt.KeepAspectRatio)

        opinion = self.current_photo.opinion
        if opinion == 'ok':
            self.opinion_ok.setChecked(True)
        elif opinion == 'bad':
            self.opinion_bad.setChecked(True)
        elif opinion == 'unsure':
            self.opinion_unsure.setChecked(True)

        self.image_draw_needed = False

        # Now fire off an update event to get the preview drawn
        self.loading_now = False
        self.handle_crop()

    def _handle_opinion(self, state):
        if self.loading_now:
            return
        
        if self.current_photo is None:
            return

        self.current_photo.update_opinion(state)

    def handle_opinion_ok(self):
        self._handle_opinion('ok')
        
    def handle_opinion_bad(self):
        self._handle_opinion('bad')
        
    def handle_opinion_unsure(self):
        self._handle_opinion('unsure')

    def handle_rotate(self, angle):
        if self.current_image is not None and not self.loading_now:
            self.image_draw_needed = True
            self.current_image.set_rotation(angle)
            self.current_photo.update_rotation(angle)

    def handle_brightness(self, brightness):
        if self.current_image is not None and not self.loading_now:
            self.image_draw_needed = True
            self.current_image.set_brightness(brightness / 127)
            self.current_photo.update_brightness(brightness / 127)

    def handle_contrast(self, contrast):
        if self.current_image is not None and not self.loading_now:
            self.image_draw_needed = True
            self.current_image.set_contrast(contrast / 127)
            self.current_photo.update_contrast(contrast / 127)

    def handle_gamma(self, gamma):
        if self.current_image is not None and not self.loading_now:
            self.image_draw_needed = True
            self.current_image.set_gamma(gamma / 10)
            self.current_photo.update_gamma(gamma / 10)

    def handle_crop(self):
        if self.loading_now:
            return
        
        if self.current_photo is None:
            return

        pixmap = self.main_pixmap.pixmap()
        pixmap = pixmap.copy(self.crop_frame.cropping_rect())

        orig_width, orig_height = self.current_image.orig_size()
        orig_size = orig_width * orig_height
        new_size = pixmap.width() * pixmap.height()

        pixmap = pixmap.scaled(102, 136, QtCore.Qt.KeepAspectRatio)
        self.preview_image.setPixmap(pixmap)

        size_change = new_size / orig_size
        self.percent_change.setText('%d%%' % int(100*size_change))
        if new_size < 5000:
            self.pixel_count.setText("<font color='red'><b>%d pixels</b></font>" % new_size)
        else:
            self.pixel_count.setText("%d pixels" % new_size)

    def set_ef_ops_enabled(self, enabled):
        self.action_fetch.setEnabled(enabled)
        self.action_upload.setEnabled(enabled)

    def handle_fetch_wizard(self):
        self.fetch_wizard = FetchWizard()
        self.fetch_wizard.start_fetch.connect(self.fetcher.start_fetch)
        self.fetch_wizard.rejected.connect(self.handle_fetch_rejected)
        self.fetch_wizard.start_fetch_reports.connect(self.reportsfetcher.start_fetch)
        self.reportsfetcher.completed.connect(self.fetch_wizard.reports_ready)
        self.fetch_wizard.show()
        self.set_ef_ops_enabled(False)

    def handle_fetch_rejected(self):
        self.set_ef_ops_enabled(True)

    def handle_fetch_completed(self):
        self.set_ef_ops_enabled(True)
        self.status_finished()

    def handle_reportsfetch_error(self, err):
        print >>sys.stderr, err
        QtGui.QMessageBox.information(self, "Error during fetch of reports list", err)

    def handle_fetch_error(self, err):
        print >>sys.stderr, err
        QtGui.QMessageBox.information(self, "Error during fetch", err)
        self.set_ef_ops_enabled(True)
        self.status_finished()

    def handle_fetch_progress(self, text, cur, max):
        self.status_start(text, max)
        self.progress.setValue(cur)

    def handle_upload_wizard(self):
        self.upload_wizard.ef_username.setText(self.settings.value('ef-username', '').toString())
        # XXX: move to "change of current_person" functions
        self.upload_wizard.upload_photos_thisone.setEnabled(self.current_person is not None)
        self.upload_wizard.show()
        self.set_ef_ops_enabled(False)

    def handle_upload_rejected(self):
        self.set_ef_ops_enabled(True)

    def handle_upload(self):
        self.upload_wizard.restart()
        
        if self.upload_wizard.upload_photos_thisone.isChecked() and self.current_person is not None:
            upload_photos = {'mode': 'list', 'people': [self.current_person.id]}
        elif self.upload_wizard.upload_photos_bysize.isChecked():
            upload_photos = {'mode': 'percent', 'filter': int(self.upload_wizard.upload_photos_minsize.text())}
        elif self.upload_wizard.upload_photos_all.isChecked():
            upload_photos = {'mode': 'good'}
        else:
            return
        username = str(self.upload_wizard.ef_username.text())
        password = str(self.upload_wizard.ef_password.text())

        QtCore.QSettings().setValue('ef-username', username)

        self.uploader.start_upload(upload_photos, username, password)

    def handle_upload_completed(self):
        self.set_ef_ops_enabled(True)
        self.status_finished()

    def handle_upload_error(self, err):
        print >>sys.stderr, err
        QtGui.QMessageBox.information(self, "Error during upload", err)
        self.set_ef_ops_enabled(True)
        self.status_finished()

    def handle_upload_progress(self, text, cur, max):
        self.status_start(text, max)
        self.progress.setValue(cur)

    def handle_db_created(self, table, key):
        if table == 'person':
            # This abstraction is very leaky - can't preserve type information through ef.db
            id, ok = key['id'].toInt()
            item = self.image_list_items[id] = ImageListItem(self.photodownloader, self.list_photo_cache, id)
            self.person_model.appendRow(item)
        elif table == 'event':
            id, ok = key['id'].toInt()
            event = self.events[id] = Event(id)
            handler = self.event_load_handlers[id] = lambda: self.filter_event.addItem(event.name, id)
            self.events[id].updated.connect(handler)

    def handle_db_exception(self, e, msg):
        print >>sys.stderr, msg
        QtGui.QMessageBox.information(self, "Error while accessing database", msg)

    def handle_db_existing_done(self, table):
        if table == 'person':
            # Hook up the signals that we didn't want to fire while the database was loading (too many pointless repetitions)
            self.person_list.setModel(self.person_model_proxy)
            self.person_list.selectionModel().currentChanged.connect(self.handle_select)
            self.person_model.itemChanged.connect(self.handle_model_item_changed)

    def handle_filter_event_changed(self, index):
        id = self.filter_event.itemData(index).toPyObject()
        self.person_model_proxy.set_event_id(id)

    def handle_openeventsforce(self):
        if self.current_person is not None:
            QtGui.QDesktopServices.openUrl(QtCore.QUrl('https://www.eventsforce.net/libdems/backend/home/codEditMain.csp?codReadOnly=1&personID=%d&curPage=1' % self.current_person.id))

    def handle_chooseeditor(self):
        if self.chooseeditor.exec_():
            QtCore.QSettings().setValue('chooseeditor-state', self.chooseeditor.saveState())
            filenames = self.chooseeditor.selectedFiles()
            self.image_editor = str(filenames[0]) if filenames else None
            QtCore.QSettings().setValue('image-editor', self.image_editor)

    def handle_editimage(self):
        if self.current_person is None or self.current_photo is None:
            return

        path = self.current_photo.full_path()
        if not os.path.exists(path):
            return

        if not self.image_editor:
            self.handle_chooseeditor()

        if not self.image_editor:
            return

        f = open(path, 'rb')
        tf = tempfile.NamedTemporaryFile(prefix='%d_%s_' % (self.current_person.id, self.current_person.fullname), suffix='.jpg', delete=False)
        shutil.copyfileobj(f, tf)
        tf.close()

        proc = QtCore.QProcess(self)
        proc.setProcessChannelMode(QtCore.QProcess.ForwardedChannels)
        proc.start(self.image_editor, [tf.name])

        pid = proc.pid()
        self.procs[pid] = proc
        proc.finished.connect(lambda: self.handle_process_finished(pid))

    def handle_process_finished(self, pid):
        proc = self.procs.pop(pid, None)

    def handle_import(self):
        if self.current_person is None:
            return

        self.action_importphoto.setEnabled(False)

        if not self.openimage.exec_():
            return

        QtCore.QSettings().setValue('openimage-state', self.openimage.saveState())

        filenames = self.openimage.selectedFiles()
        filename = str(filenames[0])
        try:
            Image.open(filename).verify()
        except Exception, e:
            QtGui.QMessageBox.information(self, "Error loading image", str(e))

        local_filename = stash_photo(filename)

        self.import_batch = Batch()
        self.import_fetchedphoto = FetchedPhoto(self.current_person.id, None, self.import_batch, local_filename=local_filename)
        self.import_fetchedphoto.run()
        self.import_batch.finished.connect(self.handle_import_finished)
        self.import_batch.finish()

    def handle_import_finished(self):
        self.action_importphoto.setEnabled(True)

    def handle_categories(self):
        for category in sorted(self.findcategories.result()):
            self.filter_category.addItem(category)

    def handle_policestatus(self):
        for status in sorted(self.findpolicestatus.result()):
            self.filter_police.addItem(status)

    def handle_filter_count(self):
        self.filter_match_count.setText(str(self.person_model_proxy.rowCount()))

def setup():
    datadir = QtGui.QDesktopServices.storageLocation(QtGui.QDesktopServices.DataLocation)
    dir = QtCore.QDir()
    if not dir.exists(datadir):
        dir.mkpath(datadir)
    setup_session(str(datadir))
    start_network_manager()
 
if __name__ == "__main__":
    QtCore.QCoreApplication.setOrganizationName('asuffield.me.uk')
    QtCore.QCoreApplication.setOrganizationDomain('asuffield.me.uk')
    QtCore.QCoreApplication.setApplicationName('ef-image-editor')
    app = QtGui.QApplication(sys.argv)
    setup()
    myapp = ImageEdit()
    myapp.show()
    rc = app.exec_()
    thread_registry.shutdown(0)
    thread_registry.wait_all()
    sys.exit(rc)
