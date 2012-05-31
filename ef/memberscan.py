from PyQt4 import QtCore
from ef.nettask import NetFuncs
from ef.task import Task, TaskList
from ef.login import LoginTask, LoginError
from ef.parser import EFDelegateParser
import traceback
import sys

voting_bool_map = {
    'yes': True,
    'no': False,
    '': False,
    'true': True,
    'false': False,
    }

class MemberParser(EFDelegateParser):
    def __init__(self, members, unreg, wrong, have_nv):
        EFDelegateParser.__init__(self)

        self.members = members
        self.unreg = unreg
        self.wrong = wrong
        self.have_nv = have_nv

    def handle_person(self, person):
        self.progress.emit('Scanned %d people' % len(self.people))
        if person.get('Membership No', '').strip() == '':
            return
        member = person['Membership No']
        self.unreg.discard(member)
        if member not in self.members:
            if self.have_nv or voting_bool_map.get(str(person['Voting Rights']).lower()):
                self.wrong.append({'person': person, 'member': None, 'msg': ['Person is not in membership list']})
        else:
            rec = self.members[member]
            errs = []
            if rec.get('surname', None) is not None and rec['surname'].strip() != person['Lastname'].strip():
                errs.append('Surname does not match membership number')
            if rec.get('voting', None) is not None:
                rec_voting = voting_bool_map.get(str(rec['voting']).lower(), False)
                person_voting = voting_bool_map.get(str(person['Voting Rights']).lower(), False)
                if rec_voting != person_voting:
                    errs.append('Discrepency in voting rights')
            if rec.get('lp', None) is not None and rec['lp'].strip() != person['Local Party'].strip():
                errs.append('Local party does not match membership number')
            if len(errs):
                self.wrong.append({'person': person, 'member': rec, 'msg': errs})

class ScanError(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return self.msg

class MemberReportTask(Task, NetFuncs):
    def __init__(self, members, unreg, wrong, have_nv):
        Task.__init__(self)
        NetFuncs.__init__(self)

        self.parser = MemberParser(members, unreg, wrong, have_nv)

    def task(self):
        self.parser.progress = self.progress
        self.progress.emit('Running report')
        soup = yield self.get('https://www.eventsforce.net/libdems/backend/home/dynaRepRun.csp?profileID=65', timeout=None)

        img = soup.find('img', title='Export to Excel')
        if img is None:
            raise ScanError("Failed to parse response from eventsforce (didn't have Export link)")
        link = img.parent

        self.progress.emit('Downloading results')

        self.report_exception = None

        self.report_op = self.get_raw(link['href'], timeout=120)
        self.report_op.reply.readyRead.connect(self.report_get_data)
        yield self.report_op

        if self.report_exception is not None:
            raise self.report_exception, None, self.report_tb

    def report_get_data(self):
        try:
            self.parser.feed(self.report_op.result())
        except Exception, e:
            self.report_exception = e
            self.report_tb = sys.exc_info()[2]
            print >>sys.stderr, traceback.format_exc()

class MemberScanner(TaskList):
    progress = QtCore.pyqtSignal(str)
    
    def __init__(self, username, password, config):
        self.members = {}
        self.unregistered_members = set()
        self.wrong_status_members = []

        self.have_non_voting = False

        for c in config:
            cols = c['cols']
            if cols['voting'] != True:
                self.have_non_voting = True
            for row in c['data'].rows:
                member = {}
                for col, val in cols.iteritems():
                    if val is None:
                        pass
                    elif val is True:
                        member[col] = True
                    else:
                        member[col] = row[val]
                if 'voting' in member and voting_bool_map.get(str(member['voting']).lower(), False):
                    self.unregistered_members.add(member['member'])
                self.members[ member['member'] ] = member

        report = MemberReportTask(self.members, self.unregistered_members, self.wrong_status_members, self.have_non_voting)

        tasks = [LoginTask(username, password), report]
        TaskList.__init__(self, tasks)

        report.progress = self.progress

    def unregistered(self):
        return self.unregistered_members

    def errors(self):
        return self.wrong_status_members
