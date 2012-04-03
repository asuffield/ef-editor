#!/usr/bin/python -d
 
import sys
import os
import traceback
import sqlalchemy
from PyQt4 import QtCore, QtGui
from image_edit import Ui_ImageEdit
from efdb import Session, Person, Photo, setup_session
from eflib import PhotoCache, FindUnsure, thread_registry
from collections import deque

class ImageListItem(QtGui.QStandardItem):
    def __init__(self, photo_cache, person):
        QtGui.QStandardItem.__init__(self)

        self.person = person
        self.photo_cache = photo_cache

        self.id = person.id
        self.tooltip = unicode(person)
        self.name = u"%s %s" % (person.firstname, person.lastname)
        self.sortname = u"%s %s" % (person.lastname, person.firstname)
        self.size_hint = QtCore.QSize(80, 160)
        self.thumbnail_size = QtCore.QSize(60, 80)

        self.setFlags(QtCore.Qt.ItemIsEnabled | QtCore.Qt.ItemIsSelectable)

    def data(self, role):
        if role == QtCore.Qt.DecorationRole:
            return self.photo()

        if role == QtCore.Qt.DisplayRole:
            return self.name

        if role == QtCore.Qt.ToolTipRole:
            return self.tooltip

        if role == QtCore.Qt.StatusTipRole:
            return self.tooltip

        if role == QtCore.Qt.SizeHintRole:
            return self.size_hint

        if role == QtCore.Qt.UserRole:
            return self.id

        if role == QtCore.Qt.UserRole+1:
            return self.sortname

        return None

    def handle_photo_ready(self, id, image):
        self.emitDataChanged()

    def handle_photo_fail(self, id, error):
        self.emitDataChanged()

    def photo(self):
        pixmap = self.photo_cache.peek_image(self.id)
        if pixmap is not None:
            return pixmap

        if self.person.current_photo is None:
            return None

        # When this finishes, we'll just update the item in the list
        # so this function gets called again, and hopefully the pixmap
        # we wanted will now be in the cache

        # (There's a bug here in the pathological case where we cycle
        # through loading more images than the cache can fit - screw it)
        
        self.photo_cache.load_image(self.id,
                                    self.person.current_photo.full_path(),
                                    ready_cb=self.handle_photo_ready,
                                    fail_cb=self.handle_photo_fail,
                                    scale_size=self.thumbnail_size)
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

        self.photo.crop_centre_x = x / self.image_width
        self.photo.crop_centre_y = y / self.image_height
        self.photo.crop_scale = self.scale_factor

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

class ImageEdit(QtGui.QMainWindow, Ui_ImageEdit):
    output_updated = QtCore.pyqtSignal()
    wheel_event = QtCore.pyqtSignal(int)

    def __init__(self, parent=None):
        QtGui.QWidget.__init__(self, parent)
        self.setupUi(self)
        
        self.back.setIcon(self.style().standardIcon(QtGui.QStyle.SP_ArrowBack))
        self.forwards.setIcon(self.style().standardIcon(QtGui.QStyle.SP_ArrowForward))
        self.back.setDisabled(True)
        self.forwards.setDisabled(True)

        self.list_photo_cache = PhotoCache(100)
        self.main_photo_cache = PhotoCache(10)
        self.unsure = FindUnsure()
        self.current_photo = None
        self.loading_now = False

        self.history_back = deque()
        self.history_forwards = deque()
        self.suppress_history = False

        self.image_list_items = {}

        self.edit_scene = QtGui.QGraphicsScene(self)
        self.main_image.setScene(self.edit_scene)
        self.main_pixmap = EditPixmap(self.wheel_event)
        self.edit_scene.addItem(self.main_pixmap)

        self.crop_frame = CropFrame(self.main_pixmap, self.output_updated)

        self.main_pixmap.setZValue(1)
        self.crop_frame.setZValue(2)

        self.session = Session()

        self.person_model = QtGui.QStandardItemModel()
        self.person_model_proxy = QtGui.QSortFilterProxyModel(self)
        self.person_model_proxy.setSourceModel(self.person_model)
        self.personList.setModel(self.person_model_proxy)

        for p in self.session.query(Person).filter(Person.current_photo!=None).all():
            self.image_list_items[p.id] = ImageListItem(self.list_photo_cache, p)
            self.person_model.appendRow(self.image_list_items[p.id])

        self.person_model_proxy.setFilterCaseSensitivity(False)
        self.person_model_proxy.setSortCaseSensitivity(False)
        self.person_model_proxy.setSortRole(QtCore.Qt.UserRole+1)
        self.person_model_proxy.sort(0)

        self.session.commit()

        self.personList.selectionModel().currentChanged.connect(self.handle_select)
        self.output_updated.connect(self.handle_crop)
        self.wheel_event.connect(self.crop_frame.handle_wheel)
        self.opinion_ok.clicked.connect(self.handle_opinion_ok)
        self.opinion_bad.clicked.connect(self.handle_opinion_bad)
        self.opinion_unsure.clicked.connect(self.handle_opinion_unsure)
        self.next_unchecked.clicked.connect(self.handle_next_unsure)
        self.back.clicked.connect(self.handle_back)
        self.forwards.clicked.connect(self.handle_forwards)
        self.search.clicked.connect(self.handle_search)
        self.search_for.returnPressed.connect(self.handle_search)

        self.commit_timer = QtCore.QTimer(self)
        self.commit_timer.setInterval(1000)
        self.commit_timer.timeout.connect(self.handle_commit_timer)
        self.commit_timer.start()

    def handle_commit_timer(self):
        self.session.commit()

    def clear_image(self):
        self.current_photo = None
        self.crop_frame.hide()
        self.main_pixmap.hide()
        self.preview_image.setPixmap(QtGui.QPixmap())
        self.person_name.setText(u'')

    def foreach_item(self, f):
        for item in self.image_list_items.itervalues():
            f(item)

    def handle_search(self):
        query = str(self.search_for.text()).strip()

        # If the search box is empty, we want to show all items
        if len(query) == 0:
            self.person_model_proxy.setFilterRegExp('')
            self.person_model_proxy.sort(0)
            return

        # If the query is just a number, treat it as a person ID query
        try:
            id = int(query)
        except ValueError:
            id = None

        if id is not None:
            self.person_model_proxy.setFilterRole(QtCore.Qt.UserRole)
            self.person_model_proxy.setFilterRegExp('^%d$' % id)
        else:
            self.person_model_proxy.setFilterRole(QtCore.Qt.DisplayRole)
            self.person_model_proxy.setFilterFixedString(query)
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

        self.load_person(current.id)

    """Load this person's photo into the editor"""
    def load_person(self, id):
        p = Person.by_id(id)

        # Suppress history updates when changing the item with
        # back/forwards, because they have special handling
        if not self.suppress_history and self.current_photo is not None:
            self.history_back.append(self.current_photo.person.id)
            self.back.setDisabled(False)
            self.history_forwards.clear()
            self.forwards.setDisabled(True)
            while len(self.history_back) > 10:
                self.history_back.popleft()

        self.next_unchecked.setDisabled(False)

        photo = p.current_photo
        if photo is None:
            self.clear_image()
        else:
            self.current_photo = photo
            self.person_name.setText(u'Loading %s...' % p)

            self.main_photo_cache.load_image(photo.id, photo.full_path(),
                                             ready_cb=self.handle_photo_ready,
                                             fail_cb=self.handle_photo_fail,
                                             )

    """Select this id in personList"""
    def select_person(self, id):
        index = self.image_list_items[id].index()
        if not index.isValid():
            return False
        index = self.person_model_proxy.mapFromSource(index)
        if not index.isValid():
            return False
        self.personList.selectionModel().setCurrentIndex(index, QtGui.QItemSelectionModel.ClearAndSelect)
        return True

    """Get us to this id, in whatever way makes sense"""
    def jump_to_person(self, id):
        if not self.select_person(id):
            self.load_person(id)

    def handle_back(self):
        if self.current_photo is not None:
            self.history_forwards.append(self.current_photo.person.id)
            self.forwards.setDisabled(False)
        if len(self.history_back) > 0:
            id = self.history_back.pop()
            self.suppress_history = True
            self.jump_to_person(id)
            self.suppress_history = False
        if len(self.history_back) == 0:
            self.back.setDisabled(True)

    def handle_forwards(self):
        if self.current_photo is not None:
            self.history_back.append(self.current_photo.person.id)
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
            self.clear_image()
            self.person_name.setText(u'Failed to load %s: %s' % (self.current_photo.person, error))

    def handle_photo_ready(self, id, pixmap):
        if self.current_photo is None or self.current_photo.id != id:
            return

        # Suppress re-entrant noise, because loading a photo emits
        # signals like editing would
        self.loading_now = True
        self.main_pixmap.setPixmap(pixmap)
        self.main_image.fitInView(self.main_pixmap, QtCore.Qt.KeepAspectRatio)
        width = pixmap.width()
        height = pixmap.height()
        self.edit_scene.setSceneRect(0, 0, width, height)
        self.crop_frame.setup_new_image(width, height, self.current_photo)
        self.main_pixmap.show()
        self.person_name.setText(unicode(self.current_photo.person))

        opinion = self.current_photo.opinion
        if opinion == 'ok':
            self.opinion_ok.setChecked(True)
        elif opinion == 'bad':
            self.opinion_bad.setChecked(True)
        elif opinion == 'unsure':
            self.opinion_unsure.setChecked(True)

        # Now fire off an update event to get the preview drawn
        self.loading_now = False
        self.handle_crop()

    def _handle_opinion(self, state):
        if self.loading_now:
            return
        
        if self.current_photo is None:
            return

        self.current_photo.opinion = state

    def handle_opinion_ok(self):
        self._handle_opinion('ok')
        
    def handle_opinion_bad(self):
        self._handle_opinion('bad')
        
    def handle_opinion_unsure(self):
        self._handle_opinion('unsure')

    def handle_next_unsure(self):
        self.next_unchecked.setDisabled(True)
        self.unsure.next(self.handle_unsure_result)

    def handle_unsure_result(self, photo):
        if photo is None:
            self.clear_image()
            self.person_name.setText("End of unsure photos")
        else:
            self.jump_to_person(photo.person.id)

    def handle_crop(self):
        if self.loading_now:
            return
        
        if self.current_photo is None:
            return

        pixmap = self.main_pixmap.pixmap()
        pixmap = pixmap.copy(self.crop_frame.cropping_rect())
        pixmap = pixmap.scaled(60, 80, QtCore.Qt.KeepAspectRatio)
        self.preview_image.setPixmap(pixmap)

def setup():
    datadir = QtGui.QDesktopServices.storageLocation(QtGui.QDesktopServices.DataLocation) + '/asuffield.me.uk/ef-image-editor/'
    dir = QtCore.QDir()
    if not dir.exists(datadir):
        dir.mkpath(datadir)
    setup_session(str(datadir))
 
if __name__ == "__main__":
    setup()
    app = QtGui.QApplication(sys.argv)
    myapp = ImageEdit()
    myapp.show()
    rc = app.exec_()
    thread_registry.shutdown(0)
    thread_registry.wait_all()
    myapp.session.commit()
    sys.exit(rc)
