from PyQt4 import QtCore, QtGui
import csv
import re

def unicode_csv_reader(f, dialect=csv.excel, **kwargs):
    csv_reader = csv.reader(f, dialect=dialect, **kwargs)
    for row in csv_reader:
        # decode UTF-8 back to Unicode, cell by cell:
        yield [unicode(cell, 'utf-8') for cell in row]

class MemberFile(object):
    def __init__(self, filename):
        self.filename = filename
        f = open(filename)
        reader = unicode_csv_reader(f)
        self.rows = list(reader)
        self.header = self.rows.pop(0)
        self.header_map = dict([(h,i) for i,h in enumerate(self.header)])

        self.cols = {'member': None,
                     'surname': None,
                     'lp': None,
                     'voting': None,
                     }

        for col in self.header:
            if re.search(r'Surname', col):
                self.cols['surname'] = col
            elif re.search(r'Local Party', col):
                self.cols['lp'] = col
            elif re.search(r'Voting', col):
                self.cols['voting'] = col
            elif re.search(r'Member', col):
                self.cols['member'] = col

    def make_row(self):
        # This is an ugly mess, but it works
        labels = [x if x is not None else u'' for x in [self.filename, self.cols['member'], self.cols['surname'], self.cols['lp'], self.cols['voting']]]
        if labels[4] == '':
            labels[4] = 'Not in file'
        items = map(QtGui.QStandardItem, labels)
        items[0].setFlags(QtCore.Qt.ItemIsSelectable | QtCore.Qt.ItemIsEnabled)
        for item in items:
            item.setData(self)
        self.model_row = items
        return items

    def make_config(self):
        cols = {'member': 1,
                'surname': 2,
                'lp': 3,
                'voting': 4,
                }

        for col in cols:
            x = str(self.model_row[ cols[col] ].text())
            if x == '' or x == 'Not in file':
                cols[col] = None
            elif x == 'All in file':
                cols[col] = True
            else:
                cols[col] = self.header_map[x]
        
        return {'data': self,
                'cols': cols,
                }
