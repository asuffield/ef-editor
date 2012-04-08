import sqlalchemy
import os
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, Boolean, Enum, ForeignKey, DateTime
from sqlalchemy.orm import scoped_session, sessionmaker, relationship
from datetime import datetime

engine = None
photodir = None
Base = declarative_base()
Session = scoped_session(sessionmaker())

class Event(Base):
    __tablename__ = 'event'

    id = Column(Integer, primary_key=True)
    name = Column(String)

    def __init__(self, id, name):
        self.id = id
        self.name = name

    @classmethod
    def by_id(self, id, session=Session):
        return session.query(Event).filter_by(id=id).first()

    def __str__(self):
        return self.name

class Photo(Base):
    __tablename__ = 'photo'

    id = Column(Integer, primary_key=True)
    url = Column(String)
    date_fetched = Column(DateTime)
    
    person_id = Column(Integer, ForeignKey('person.id'))
    person = relationship('Person', primaryjoin='Photo.person_id==Person.id', backref='photos')

    crop_centre_x = Column(Float)
    crop_centre_y = Column(Float)
    crop_scale = Column(Float)
    rotate = Column(Float)

    opinion = Column(Enum('ok', 'bad', 'unsure', name='photo_opinion'), default='unsure')

    def __init__(self, person):
        self.person = person
        self.url = None
        self.crop_centre_x = 0.5
        self.crop_centre_y = 0.5
        self.crop_scale = 1
        self.rotate = 0
        self.opinion = 'unsure'

    @classmethod
    def by_id(self, id, session=Session):
        return session.query(Photo).filter_by(id=id).first()

    def url_filename(self):
        return self.url.split('/')[-1]

    def full_path(self):
        return os.path.join(photodir, self.url_filename())

    def open(self, *args, **kwargs):
        return open(self.full_path(), *args, **kwargs)

class Person(Base):
    __tablename__ = 'person'

    id = Column(Integer, primary_key=True)
    firstname = Column(String)
    lastname = Column(String)
    title = Column(String)
    fullname = Column(String)

    current_photo_id = Column(Integer, ForeignKey('photo.id', name='fk_current_photo', use_alter=True))
    current_photo = relationship('Photo', primaryjoin=current_photo_id==Photo.id, post_update=True)

    last_checked_at = Column(DateTime)

    def __init__(self, id):
        self.id = id
        self.firstname = ''
        self.lastname = ''
        self.title = ''
        self.fullname = ''
        self.current_photo = None
        self.last_checked_at = datetime.now()

    @classmethod
    def by_id(self, id, session=Session):
        return session.query(Person).filter_by(id=id).first()

    def __repr__(self):
        return u"Person<%d: %s %s %s>" % (self.id, self.title, self.firstname, self.lastname)
    
    def __str__(self):
        return u"%d: %s" % (self.id, self.fullname)

class Registration(Base):
    __tablename__ = 'registration'

    person_id = Column(Integer, ForeignKey('person.id'), primary_key=True)
    event_id = Column(Integer, ForeignKey('event.id'), primary_key=True)

    person = relationship('Person', backref='events')
    event = relationship('Event', backref='people')

    attendee_type = Column(String)
    booking_ref = Column(String)
    booker_email = Column(String)
    booker_firstname = Column(String)
    booker_lastname = Column(String)

    def __init__(self, person, event):
        self.person = person
        self.event = event

def setup_session(datadir):
    global engine, photodir
    photodir = os.path.join(datadir, 'photos')
    if not os.path.exists(photodir):
        os.mkdir(photodir)
    dbfile = os.path.join(datadir, 'people.sqlite')
    engine = sqlalchemy.create_engine('sqlite:///%s' % dbfile.replace('\\', '\\\\'))
    Session.configure(bind=engine)
    Base.metadata.create_all(bind=Session.bind)
