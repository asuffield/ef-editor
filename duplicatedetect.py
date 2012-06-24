#!/usr/bin/python

from __future__ import division

import sys
import csv
from PyQt4 import QtCore, QtGui
from datetime import datetime

from ef.ui.duplicatedetect import Ui_DuplicateDetect
from ef.netlib import start_network_manager, stop_network_manager

class DuplicateDetect(QtGui.QMainWindow, Ui_DuplicateDetect):
    def __init__(self, parent=None):
        super(QtGui.QWidget, self).__init__(parent)
        self.setupUi(self)

        self.settings = QtCore.QSettings()

        self.username.setText(self.settings.value('ef-username', '').toString())

        self.start.clicked.connect(self.handle_run)

        self.status_timer = QtCore.QTimer(self)
        self.status_timer.setInterval(500)
        self.status_timer.timeout.connect(self.status_timer_update)

    def status_elapsed_str(self):
        td = datetime.now() - self.status_started
        minutes,seconds = divmod(td.seconds, 60)
        return "%d:%02d" % (minutes,seconds)

    def status_timer_update(self):
        if self.status_started is not None:
            self.status.setText(self.status_elapsed_str())

    def handle_run(self):
        username = self.username.text()
        password = self.password.text()

        QtCore.QSettings().setValue('ef-username', username)

        self.scanner = DuplicateDetector(username, password)
        self.status_started = datetime.now()
        self.status_timer.start()
        self.start.setEnabled(False)
        self.scanner.task_finished.connect(self.handle_finished)
        self.scanner.task_exception.connect(self.handle_exception)
        self.scanner.progress.connect(self.handle_progress)

        self.scanner.start_task()

    def task_ended(self):
        self.start.setEnabled(True)
        self.status_started = None
        self.status_timer.stop()
        self.progress.setText('Done')

    def handle_finished(self):
        self.task_ended()

        for model in self.models.itervalues():
            model.removeRows(0, model.rowCount())
        
        for member in self.scanner.unregistered_members:
            rec = self.scanner.members[member]
            self.results_model.appendRow([QtGui.QStandardItem(rec['member']),
                                          QtGui.QStandardItem(rec['surname']),
                                          QtGui.QStandardItem('Unregistered voting rep'),
                                          ])
            self.models['unregistered'].appendRow([QtGui.QStandardItem(rec['member']),
                                                   QtGui.QStandardItem(rec['surname']),
                                                   ])

        for err in self.scanner.wrong_status_members:
            person = err['person']
            msgs = err['msg']
            name = '%s %s %s' % (person['Salutation'], person['Firstname'], person['Lastname'])
            self.results_model.appendRow([QtGui.QStandardItem(person['Membership No']),
                                          QtGui.QStandardItem(name),
                                          QtGui.QStandardItem('\n'.join(map(lambda m: m[1], msgs))),
                                          ])
            sorted_msgs = {}
            for kind, msg in msgs:
                sorted_msgs.setdefault(kind, []).append(msg)

            for kind, msgs in sorted_msgs.iteritems():
                model = self.models[kind]
                row = [QtGui.QStandardItem(person['Membership No']),
                       QtGui.QStandardItem(name),
                       ]
                if model.columnCount() >= 3:
                    row.append(QtGui.QStandardItem('\n'.join(msgs)))
                model.appendRow(row)
            
        for view in self.views.itervalues():
            if view.model().columnCount() == 2:
                view.sortByColumn(1, QtCore.Qt.AscendingOrder)
            else:
                view.sortByColumn(2, QtCore.Qt.AscendingOrder)
            view.resizeColumnToContents(0)
            view.resizeColumnToContents(1)

    def handle_exception(self, e, msg, blob):
        print >>sys.stderr, msg
        QtGui.QMessageBox.information(self, "Error while scanning members", msg)
        self.task_ended()

if __name__ == "__main__":
    QtCore.QCoreApplication.setOrganizationName('asuffield.me.uk')
    QtCore.QCoreApplication.setOrganizationDomain('asuffield.me.uk')
    QtCore.QCoreApplication.setApplicationName('ef-duplicatedetect')
    app = QtGui.QApplication(sys.argv)
    start_network_manager()
    myapp = DuplicateDetect()
    myapp.show()
    rc = app.exec_()
    stop_network_manager()
    sys.exit(rc)
