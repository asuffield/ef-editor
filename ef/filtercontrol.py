from __future__ import division

from PyQt4 import QtCore, QtGui
from PIL import Image
from ef.db import Photo
import os
import traceback

class FilterProxyModel(QtGui.QSortFilterProxyModel):
    def __init__(self):
        QtGui.QSortFilterProxyModel.__init__(self)
        self.name = ''
        self.id = None
        self.only_bad_sizes = False
        self.only_missing = False
        self.opinion = 'unsure'
        self.police_status = 'any'
        self.event_id = None
        self.category = 'any'

    def set_opinion(self, opinion):
        self.opinion = opinion
        self.invalidateFilter()

    def set_event_id(self, event_id):
        self.event_id = event_id
        self.invalidateFilter()

    def set_police_status(self, status):
        self.police_status = status
        self.invalidateFilter()

    def set_only_bad_sizes(self, state):
        self.only_bad_sizes = state
        self.invalidateFilter()

    def set_only_missing(self, state):
        self.only_missing = state
        self.invalidateFilter()

    def set_category(self, category):
        self.category = category
        self.invalidateFilter()

    def is_size_ok(self, index):
        model = self.sourceModel()

        size = model.data(index, QtCore.Qt.UserRole+5).toPyObject()
        if size is None:
            return True

        width, height = size
        if width * height == 0:
            photo_id = model.data(index, QtCore.Qt.UserRole+7).toPyObject()
            path = model.data(index, QtCore.Qt.UserRole+6).toPyObject()
            if path is not None and photo_id is not None and os.path.exists(path):
                # Fix up the database by reading the image
                image = Image.open(str(path))
                width, height = image.size
                Photo.upsert({'id': photo_id, 'width': width, 'height': height})
            else:
                return False

        if (width * height) < 20000:
            return False

        space_used = (6 * height) / (8 * width)
        if space_used > 1:
            space_used = 1 / space_used

        if space_used < 0.95:
            return False

        return True

    def filterAcceptsRow(self, source_row, source_parent):
        try:
            index = self.sourceModel().index(source_row, 0, source_parent)
            return self.is_row_ok(index)
        except:
            traceback.print_exc()

    def is_row_ok(self, index):
        model = self.sourceModel()

        if self.id is not None:
            id, ok = model.data(index, QtCore.Qt.UserRole).toInt()
            return self.id == id

        if self.name:
            name = model.data(index, QtCore.Qt.DisplayRole).toPyObject()
            if name is not None and unicode(self.name).lower() not in unicode(name).lower():
                return False

        if self.only_missing:
            size = model.data(index, QtCore.Qt.UserRole+5).toPyObject()
            if size is not None:
                x,y = size
                if x > 0 and y > 0:
                    return False

        if self.only_bad_sizes:
            if self.is_size_ok(index):
                return False

        if self.opinion != 'any':
            opinion = model.data(index, QtCore.Qt.UserRole+2).toPyObject()
            if opinion is not None and opinion != self.opinion:
                return False

        if self.police_status != 'any':
            police_status = model.data(index, QtCore.Qt.UserRole+3).toString()
            if police_status is not None and police_status != self.police_status:
                return False

        if self.category != 'any' or self.event_id:
            registrations = model.data(index, QtCore.Qt.UserRole+4).toList()
            if registrations is not None:
                matched_reg = False
                for variant in registrations:
                    registration = variant.toPyObject()
                    if self.category != 'any':
                        if registration.attendee_type != self.category:
                            continue
                    if self.event_id:
                        if registration.event_id != self.event_id:
                            continue
                    matched_reg = True
                    break
                if not matched_reg:
                    return False

        return True
