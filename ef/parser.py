from HTMLParser import HTMLParser
import sys
import re

person_fields = set(['Person ID', 'Firstname', 'Lastname', 'Full Name', 'Salutation', 'username',
                     'Profile Picture', 'Membership No', 'Voting Rights',
                     'EF_Application Status', 'common first name'])
event_fields = set(['Bookers Firstname', 'Bookers lastname', 'Booking Ref', 'Type of Attendee'])

class EFParser(HTMLParser):
    current_target = None
    current_inserter = None
    keys = None
    #data = []
    pos = None
    records = 0
    current_string = None
    
    def handle_starttag(self, tag, attrs):
        if tag == 'tr':
            if self.keys is None:
                self.keys = []
            else:
                self.current_target = {}
                self.pos = 0
                self.records = self.records + 1
                #self.data.append(self.current_target)

            #if len(self.data) % 1000 == 0:
            #    print >>sys.stderr, "Parsed %d rows" % len(self.data)

        elif tag == 'td':
            if self.current_target is None:
                self.current_inserter = lambda data: self.keys.append(data)
            else:
                i = self.pos
                self.pos = self.pos + 1
                def inserter(data):
                    if i >= len(self.keys):
                        raise ValueError('Eventsforce sent corrupted data in report, in record %d. Record so far: %s' % (self.records, self.current_target))
                    j = self.keys[i]
                    self.current_target[j] = data
                self.current_inserter = inserter
            self.current_string = ''

    def handle_endtag(self, tag):
        if tag == 'tr':
            if self.current_target is not None:
                self.handle_record(self.current_target)
            #else:
            #    print self.keys
            self.current_target = None
        elif tag == 'td':
            if self.current_inserter is not None:
                self.current_inserter(self.current_string.strip())
                self.current_inserter = None
                
    def handle_data(self, data):
        if self.current_string is not None:
            self.current_string = self.current_string + ' ' + data.strip()

    def handle_record(self, record):
        pass

class EFDelegateParser(EFParser):
    people = {}
    events_map = {}
    registrations = {}
    
    def get_event(self, record):
        try:
            id = int(record.pop('Event ID'))
        except ValueError:
            return None

        name = record.pop('Event Name')
        if self.events_map.has_key(id):
            return id

        self.events_map[id] = name
        self.handle_event(id, name)
        return id

    def handle_record(self, record):
        try:
            if record['Person ID'] == '':
                return
            person_id = record['Person ID'] = int(record['Person ID'])
        except (KeyError, ValueError):
            print "Confusing nonsense in person record", record
            return

        person = self.people.setdefault(person_id, {'events': {}})

        for key in person_fields:
            if key in record:
                value = record.pop(key)
                # Suppress duplicates (take the first thing in the report), but prefer non-zero-length values
                if 0 == len(unicode(person.get(key, ''))):
                    person[key] = value
        # Record changes size here, so must not be an iterator...
        for key in list(record.keys()):
            if re.match(r'^Local Party', key):
                if record[key].strip() != '':
                    person['Local Party'] = record[key]
                del record[key]
        self.handle_person(person)

        event_id = self.get_event(record)
        if event_id is None:
            return
        events = person['events']
        event = events.setdefault(event_id, {})
        for key in event_fields:
            # Take the first thing in the report, but prefer non-zero-length values
            if record.has_key(key) and 0 == len(unicode(event.get(key, ''))):
                event[key] = record[key]

        self.handle_registration(person, event_id)
        self.registrations.setdefault(person_id, []).append(event_id)

    def handle_person(self, person):
        pass

    def handle_event(self, event_id, event):
        pass

    def handle_registration(self, person, event_id):
        pass

    def get_people(self):
        return self.people

    def get_events(self):
        return self.events_map

class EFDelegateProgressParser(EFDelegateParser):
    def handle_person(self, person):
        if len(self.people) % 1000 == 0:
            print >>sys.stderr, "Parsed %d people" % len(self.people)
