from PyQt4 import QtCore, QtGui

class FilterProxyModel(QtGui.QSortFilterProxyModel):
    def __init__(self):
        QtGui.QSortFilterProxyModel.__init__(self)
        self.name = ''
        self.allow_bad_sizes = True
        self.opinion = 'any'
        self.police_status = ''
        self.event = ''

    def filterAcceptsRow(self, source_row, source_parent):
        model = self.sourceModel()
        index = model.index(source_row, 0, source_parent)
        name = model.data(index, QtCore.Qt.DisplayRole)
        id = model.data(index, QtCore.Qt.UserRole)
        pixmap = model.data(index, QtCore.Qt.DecorationRole)
        opinion = model.data(index, QtCore.Qt.UserRole+2)
        police_status = model.data(index, QtCore.Qt.UserRole+3)
