#!/usr/bin/python

from __future__ import division

import sys

from PyQt4 import QtCore, QtGui

from ef.ui.membercheck import Ui_MemberCheck
from ef.netlib import start_network_manager, stop_network_manager
from ef.memberfile import MemberFile

class MemberCheck(QtGui.QMainWindow, Ui_MemberCheck):
    def __init__(self, parent=None):
        super(QtGui.QWidget, self).__init__(parent)
        self.setupUi(self)

        self.settings = QtCore.QSettings()

        self.input_model = QtGui.QStandardItemModel(0, 5)
        self.input_model.setHeaderData(0, QtCore.Qt.Horizontal, 'Filename')
        self.input_model.setHeaderData(1, QtCore.Qt.Horizontal, 'Surname\ncol')
        self.input_model.setHeaderData(2, QtCore.Qt.Horizontal, 'Member#\ncol')
        self.input_model.setHeaderData(3, QtCore.Qt.Horizontal, 'Local party\ncol')
        self.input_model.setHeaderData(4, QtCore.Qt.Horizontal, 'Voting\ncol')
        self.input_files.setModel(self.input_model)

        self.input_files.horizontalHeader().setResizeMode(QtGui.QHeaderView.ResizeToContents)
        self.input_files.horizontalHeader().setResizeMode(0, QtGui.QHeaderView.Stretch)

        self.results_model = QtGui.QStandardItemModel(0, 2)
        self.results_model.setHeaderData(0, QtCore.Qt.Horizontal, 'Person')
        self.results_model.setHeaderData(1, QtCore.Qt.Horizontal, 'Error')
        self.results.setModel(self.results_model)

        self.addfile = QtGui.QFileDialog(self, 'Load membership list')
        self.addfile.setFileMode(QtGui.QFileDialog.ExistingFile)
        self.addfile.setNameFilter('*.csv')
        self.addfile.restoreState(self.settings.value('addfile-state', '').toByteArray())

        self.add_input.clicked.connect(self.handle_add)
        self.remove_input.clicked.connect(self.handle_remove)

    def __del__(self):
        # This seems to stop the global destructor from spazzing out
        # and deleting objects twice
        self.addfile = None

    def handle_add(self):
        if not self.addfile.exec_():
            return

        filenames = self.addfile.selectedFiles()
        filename = str(filenames[0])
        try:
            members = MemberFile(filename)
        except Exception, e:
            QtGui.QMessageBox.information(self, "Error loading membership list", str(e))
            return

        self.input_model.appendRow(members.make_row())

    def handle_remove(self):
        index = self.input_files.selectionModel().currentIndex()
        if not index.isValid():
            return
        row = index.row()
        self.input_model.takeRow(row)

if __name__ == "__main__":
    QtCore.QCoreApplication.setOrganizationName('asuffield.me.uk')
    QtCore.QCoreApplication.setOrganizationDomain('asuffield.me.uk')
    QtCore.QCoreApplication.setApplicationName('ef-membercheck')
    app = QtGui.QApplication(sys.argv)
    start_network_manager()
    myapp = MemberCheck()
    myapp.show()
    rc = app.exec_()
    stop_network_manager()
    sys.exit(rc)
