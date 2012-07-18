from PyQt4 import QtCore, QtGui
from ef.ui.fetch_wizard import Ui_LoadPeopleWizard

class FetchWizard(QtGui.QWizard, Ui_LoadPeopleWizard):
    start_fetch = QtCore.pyqtSignal(int, QtCore.QDate, str, str, str)
    start_fetch_reports = QtCore.pyqtSignal(str, str)
    
    def __init__(self, username, password, parent=None):
        super(QtGui.QWizard, self).__init__(parent)
        self.setupUi(self)

        self.settings = settings = QtCore.QSettings()
        self.username = username
        self.password = password

        self.last_event, ok = settings.value('ef-fetch-event', '').toInt()
        if not ok:
            self.last_event = None

        self.setButtonText(QtGui.QWizard.FinishButton, 'Start download')
        self.fetch_since.setDate(QtCore.QDate.currentDate())

        self.accepted.connect(self.handle_accepted)
        self.fetch_event.currentIndexChanged.connect(self.handle_event_changed)
        self.fetch_since.dateChanged.connect(self.handle_date_changed)
        self.date_has_changed = False

        self.page(0).registerField('event*', self.fetch_event, 'currentText', self.fetch_event.currentIndexChanged)

    def show(self):
        QtGui.QWizard.show(self)

        self.fetch_event.clear()
        self.fetch_event.addItem('Loading...')
        self.fetch_event.setEnabled(False)
        self.start_fetch_reports.emit(self.username, self.password)

    @QtCore.pyqtSlot(list)
    def reports_ready(self, events_list):
        self.fetch_event.clear()
        for name, id in sorted(events_list, key=lambda p: p[1]):
            self.fetch_event.addItem(name, id)

        if self.last_event:
            index = self.fetch_event.findData(self.last_event)
            if index >= 0:
                self.fetch_event.setCurrentIndex(index)

        self.fetch_event.setEnabled(True)
        self.fetch_since.setEnabled(True)

    def handle_event_changed(self, index):
        if not self.date_has_changed:
            fetch_event = self.fetch_event.itemData(index).toPyObject()
            if fetch_event is None:
                return
            last_date = self.settings.value('last-fetched-%d' % fetch_event, '').toString()
            if len(last_date):
                self.fetch_since.setDate(QtCore.QDate.fromString(last_date, QtCore.Qt.ISODate))
            else:
                self.fetch_since.setDate(QtCore.QDate.currentDate())

    def handle_date_changed(self, date):
        self.date_has_changed = True

    def handle_accepted(self):
        fetch_event = self.fetch_event.itemData(self.fetch_event.currentIndex()).toPyObject()
        if fetch_event is None:
            fetch_event = 0

        fetch_photos = 'none'
        if self.fetch_photos_missing.isChecked():
            fetch_photos = 'missing'
        elif self.fetch_photos_all.isChecked():
            fetch_photos = 'all'

        if fetch_event:
            QtCore.QSettings().setValue('ef-fetch-event', fetch_event)

        self.start_fetch.emit(fetch_event, self.fetch_since.date(), fetch_photos, self.username, self.password)
