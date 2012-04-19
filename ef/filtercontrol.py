from PyQt4 import QtCore, QtGui
from ef.ui.filter_control import Ui_FilterGroup

class FilterControl(QtGui.QWidget, Ui_FilterGroup):
    deleted = QtCore.pyqtSignal(QtGui.QWidget)
    updated = QtCore.pyqtSignal()
    
    def __init__(self, parent=None):
        QtGui.QWidget.__init__(self, parent=parent)
        self.setupUi(self)

        self.delete_filter.clicked.connect(self.delete_this)
        self.field.currentIndexChanged.connect(self.updated)
        self.negate.currentIndexChanged.connect(self.updated)
        self.value.currentIndexChanged.connect(self.updated)

    def delete_this(self):
        self.deleted.emit(self)

    def current(self):
        field = self.field.currentText()
        negate = self.negate.currentText()
        value = self.value.currentText()

        if field == '' or value == '':
            return None

        return (field, True if negate == 'is' else False, value)

class FilterList(QtCore.QObject):
    updated = QtCore.pyqtSignal()
    
    def __init__(self, container, add_button):
        QtCore.QObject.__init__(self)

        self.container = container
        self.filters = []

        add_button.clicked.connect(self.add_filter)

        self.reset()

    def reset(self):
        for control in self.filters:
            control.deleteLater()
        self.filters = []

        self.add_filter()
        self.filters[0].label.setText('Where')
        self.filters[0].delete_filter.deleteLater()

        self.updated.emit()

    def add_filter(self):
        control = FilterControl()
        control.deleted.connect(self.delete_filter)
        self.filters.append(control)

        index = self.container.layout().count() - 2
        self.container.layout().insertWidget(index, control)

    def delete_filter(self, control):
        # OrderedHash? Overkill?
        del self.filters[ self.filters.index(control) ]
        control.deleteLater()

        self.updated.emit()
