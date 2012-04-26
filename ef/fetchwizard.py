from PyQt4 import QtCore, QtGui
from ef.ui.fetch_wizard import Ui_LoadPeopleWizard

class FetchWizard(QtGui.QWizard, Ui_LoadPeopleWizard):
    start_fetch = QtCore.pyqtSignal(int, str, str, str)
    start_fetch_reports = QtCore.pyqtSignal(str, str)
    
    def __init__(self, parent=None):
        super(QtGui.QWizard, self).__init__(parent)
        self.setupUi(self)

        settings = QtCore.QSettings()

        self.last_report, ok = settings.value('ef-fetch-report', '').toInt()
        if not ok:
            self.last_report = None

        self.ef_username.setText(settings.value('ef-username', '').toString())
        self.setButtonText(QtGui.QWizard.FinishButton, 'Start download')

        self.accepted.connect(self.handle_accepted)
        self.currentIdChanged.connect(self.handle_changed)

        self.page(0).registerField('username', self.ef_username)
        self.page(0).registerField('password*', self.ef_password)
        self.page(1).registerField('report*', self.fetch_people_report, 'currentText', self.fetch_people_report.currentIndexChanged)

    @QtCore.pyqtSlot(list)
    def reports_ready(self, reports_list):
        self.fetch_people_report.clear()
        self.fetch_people_report.addItem('None', 0)
        for name, id in reports_list:
            self.fetch_people_report.addItem(name, id)

        if self.last_report:
            index = self.fetch_people_report.findData(self.last_report)
            if index >= 0:
                self.fetch_people_report.setCurrentIndex(index)

        self.fetch_people_report.setEnabled(True)

    def handle_changed(self, page_id):
        if page_id == 1:
            username = str(self.ef_username.text())
            password = str(self.ef_password.text())

            self.fetch_people_report.clear()
            self.fetch_people_report.addItem('Loading...')
            self.fetch_people_report.setEnabled(False)
            self.start_fetch_reports.emit(username, password)

    def handle_accepted(self):
        fetch_people_report = self.fetch_people_report.itemData(self.fetch_people_report.currentIndex()).toPyObject()
        if fetch_people_report is None:
            fetch_people_report = 0

        fetch_photos = 'none'
        if self.fetch_photos_missing.isChecked():
            fetch_photos = 'missing'
        elif self.fetch_photos_all.isChecked():
            fetch_photos = 'all'
        username = str(self.ef_username.text())
        password = str(self.ef_password.text())

        QtCore.QSettings().setValue('ef-username', username)
        if fetch_people_report:
            QtCore.QSettings().setValue('ef-fetch-report', fetch_people_report)

        self.start_fetch.emit(fetch_people_report, fetch_photos, username, password)
