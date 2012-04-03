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

class Photo(Base):
    __tablename__ = 'photo'

    id = Column(Integer, primary_key=True)
    filename = Column(String)
    url = Column(String)
    date_downloaded = Column(DateTime)
    
    person_id = Column(Integer, ForeignKey('person.id'))
    person = relationship('Person', primaryjoin='Photo.person_id==Person.id', backref='photos')

    crop_centre_x = Column(Float)
    crop_centre_y = Column(Float)
    crop_scale = Column(Float)

    opinion = Column(Enum('ok', 'bad', 'unsure', name='photo_opinion'), default='unsure')

    def __init__(self, person, filename):
        self.person = person
        self.filename = filename
        self.crop_centre_x = 0.5
        self.crop_centre_y = 0.5
        self.crop_scale = 1
        self.date_downloaded = datetime.now()
        self.opinion = 'unsure'

    @classmethod
    def by_id(self, id):
        return Session.query(Photo).filter_by(id=id).first()

    def full_path(self):
        return os.path.join(photodir, self.filename)

    def open(self, *args, **kwargs):
        return open(self.full_path(), *args, **kwargs)

class Person(Base):
    __tablename__ = 'person'

    id = Column(Integer, primary_key=True)
    firstname = Column(String)
    lastname = Column(String)
    title = Column(String)

    current_photo_id = Column(Integer, ForeignKey('photo.id', name='fk_current_photo', use_alter=True))
    current_photo = relationship('Photo', primaryjoin=current_photo_id==Photo.id, post_update=True)

    last_checked_at = Column(DateTime)

    def __init__(self, person):
        self.id = int(person['Person ID'])
        self.firstname = person['Firstname']
        self.lastname = person['Lastname']
        self.title = person['Title']
        self.current_photo = None
        self.last_checked_at = datetime.now()

    @classmethod
    def by_id(self, id):
        return Session.query(Person).filter_by(id=id).first()

    @classmethod
    def by_person(self, person):
        return self.by_id(int(person['Person ID']))

    def __repr__(self):
        return u"Person<%d: %s %s %s>" % (self.id, self.title, self.firstname, self.lastname)
    
    def __str__(self):
        return u"%d: %s %s %s" % (self.id, self.title, self.firstname, self.lastname)

def setup_session(datadir):
    global engine, photodir
    photodir = os.path.join(datadir, 'photos')
    if not os.path.exists(photodir):
        os.mkdir(photodir)
    dbfile = os.path.join(datadir, 'people.sqlite')
    engine = sqlalchemy.create_engine('sqlite:///%s' % dbfile.replace('\\', '\\\\'))
    Session.configure(bind=engine)
    Base.metadata.create_all(bind=Session.bind)
