#!/usr/bin/python

from __future__ import division

import sys
import csv
from PyQt4 import QtCore, QtGui
from datetime import datetime

from ef.ui.membercheck import Ui_MemberCheck
from ef.netlib import start_network_manager, stop_network_manager
from ef.memberfile import MemberFile
from ef.memberscan import MemberScanner

class ColumnSelectDelegate(QtGui.QItemDelegate):
    def createEditor(self, parent, option, index):
        editor = QtGui.QComboBox(parent)

        return editor

    def setup_combo_entries(self, editor, memberfile):
        editor.clear()
        editor.addItem('')
        for col in memberfile.header:
            editor.addItem(col)

    def setEditorData(self, editor, index):
        memberfile = index.model().data(index, QtCore.Qt.UserRole+1).toPyObject()
        current = index.model().data(index, QtCore.Qt.EditRole).toString()

        self.setup_combo_entries(editor, memberfile)

        i = editor.findText(current)
        if i == -1:
            i = 0
        editor.setCurrentIndex(i)

    def setModelData(self, editor, model, index):
        column = editor.currentText()

        model.setData(index, column, QtCore.Qt.EditRole)

    def updateEditorGeometry(self, editor, option, index):
        editor.setGeometry(option.rect)

class VotingSelectDelegate(ColumnSelectDelegate):
    def setup_combo_entries(self, editor, memberfile):
        editor.clear()
        editor.addItem('Not in file')
        editor.addItem('All in file')
        
        for col in memberfile.header:
            editor.addItem(col)

def make_results_model(view, want_error_column=False):
    model = QtGui.QStandardItemModel(0, 3 if want_error_column else 2)
    model.setHeaderData(0, QtCore.Qt.Horizontal, 'Member#')
    model.setHeaderData(1, QtCore.Qt.Horizontal, 'Person')
    if want_error_column:
        model.setHeaderData(2, QtCore.Qt.Horizontal, 'Error')
    view.setModel(model)
    view.sortByColumn(2 if want_error_column else 1, QtCore.Qt.AscendingOrder)
    view.horizontalHeader().setResizeMode(QtGui.QHeaderView.ResizeToContents)
    view.horizontalHeader().setVisible(True)
    return model

class MemberCheck(QtGui.QMainWindow, Ui_MemberCheck):
    def __init__(self, parent=None):
        super(QtGui.QWidget, self).__init__(parent)
        self.setupUi(self)

        self.settings = QtCore.QSettings()

        self.username.setText(self.settings.value('ef-username', '').toString())

        self.input_model = QtGui.QStandardItemModel(0, 5)
        self.input_model.setHeaderData(0, QtCore.Qt.Horizontal, 'Filename')
        self.input_model.setHeaderData(1, QtCore.Qt.Horizontal, 'Member#')
        self.input_model.setHeaderData(2, QtCore.Qt.Horizontal, 'Surname')
        self.input_model.setHeaderData(3, QtCore.Qt.Horizontal, 'Local party')
        self.input_model.setHeaderData(4, QtCore.Qt.Horizontal, 'Voting')
        self.input_files.setModel(self.input_model)

        self.column_select_delegate = ColumnSelectDelegate(self.input_files)
        self.voting_select_delegate = VotingSelectDelegate(self.input_files)
        self.input_files.setItemDelegateForColumn(1, self.column_select_delegate)
        self.input_files.setItemDelegateForColumn(2, self.column_select_delegate)
        self.input_files.setItemDelegateForColumn(3, self.column_select_delegate)
        self.input_files.setItemDelegateForColumn(4, self.voting_select_delegate)

        self.input_files.horizontalHeader().setResizeMode(QtGui.QHeaderView.ResizeToContents)
        self.input_files.horizontalHeader().setResizeMode(0, QtGui.QHeaderView.Stretch)

        self.results_model = make_results_model(self.all_results, want_error_column=True)
        self.notmember_model = make_results_model(self.notmember_results)
        self.voting_model = make_results_model(self.voting_results)
        self.personaldata_model = make_results_model(self.personaldata_results, want_error_column=True)
        self.unregistered_model = make_results_model(self.unregistered_results)

        self.models = {'all': self.results_model,
                       'notmember': self.notmember_model,
                       'personaldata': self.personaldata_model,
                       'voting': self.voting_model,
                       'unregistered': self.unregistered_model,
                       }

        self.views = {'all': self.all_results,
                      'notmember': self.notmember_results,
                      'voting': self.voting_results,
                      'personaldata': self.personaldata_results,
                      'unregistered': self.unregistered_results,
                      }

        self.addfile = QtGui.QFileDialog(self, 'Load membership list')
        self.addfile.setFileMode(QtGui.QFileDialog.ExistingFile)
        self.addfile.setNameFilter('*.csv')
        self.addfile.restoreState(self.settings.value('addfile-state', '').toByteArray())

        self.savereport = QtGui.QFileDialog(self, 'Save report')
        self.savereport.setFileMode(QtGui.QFileDialog.AnyFile)
        self.savereport.setAcceptMode(QtGui.QFileDialog.AcceptSave)
        self.savereport.setNameFilter('*.csv')
        self.savereport.setDefaultSuffix('csv')
        self.savereport.restoreState(self.settings.value('savereport-state', '').toByteArray())        

        self.add_input.clicked.connect(self.handle_add)
        self.remove_input.clicked.connect(self.handle_remove)
        self.start.clicked.connect(self.handle_run)
        self.save.clicked.connect(self.handle_save)

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

    def handle_run(self):
        username = self.username.text()
        password = self.password.text()
        config = []
        for i in xrange(0, self.input_model.rowCount()):
            index = self.input_model.index(i, 0)
            memberfile = self.input_model.data(index, QtCore.Qt.UserRole+1).toPyObject()
            config.append(memberfile.make_config())

        QtCore.QSettings().setValue('ef-username', username)

        self.scanner = MemberScanner(username, password, config)
        self.status_started = datetime.now()
        self.status_timer.start()
        self.start.setEnabled(False)
        self.progress.setText('Logging in...')
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

    def handle_progress(self, msg):
        self.progress.setText(msg)

    def handle_save(self):
        view = self.results_tabs.currentWidget().findChild(QtGui.QTableView)
        model = view.model()
        
        if not self.savereport.exec_():
            return

        QtCore.QSettings().setValue('savereport-state', self.savereport.saveState())

        filenames = self.savereport.selectedFiles()
        filename = str(filenames[0])

        f = open(filename, 'wb')
        writer = csv.writer(f)

        headers = ['Member#', 'Person']
        if model.columnCount >= 3:
            headers.append('Errors')
        writer.writerow(headers)

        for i in xrange(0, model.rowCount()):
            row = []
            for j in xrange(0, model.columnCount()):
                index = model.index(i, j)
                row.append(unicode(model.data(index).toString()).encode('utf-8'))
            writer.writerow(row)
        
        f.close()

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
