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

        self.member_col = None
        self.surname_col = None
        self.lp_col = None
        self.voting_col = None

        for col in self.header:
            if re.search(r'Surname', col):
                self.surname_col = col
            elif re.search(r'Local Party', col):
                self.lp_col = col
            elif re.search(r'Voting', col):
                self.voting_col = col
            elif re.search(r'Member', col):
                self.member_col = col

    def make_row(self):
        labels = [x if x is not None else u'' for x in [self.filename, self.member_col, self.surname_col, self.lp_col, self.voting_col]]
        items = map(QtGui.QStandardItem, labels)
        for item in items:
            item.setData(self)
        return items
