from __future__ import annotations
import uuid 
from typing import List
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, ForeignKey, Integer, String, Table, UniqueConstraint, Enum, Index
from sqlalchemy.orm import relationship, Session
from sqlalchemy_utils.types import UUIDType
from sqlalchemy_utils.models import Timestamp

from ADSScanExplorerPipeline.exceptions import PageNameException
from ADSScanExplorerPipeline.utils import is_valid_uuid
import enum

Base = declarative_base()

class VolumeStatus(enum.Enum):
    """Volume ingestion status"""
    New = 1
    Processing = 2
    Update = 3
    Db_done = 4
    Bucket_done = 5
    Done = 6
    Error = 7

class PageColor(enum.Enum):
    """Page Color Type"""
    BW = 1
    Greyscale = 2
    Color = 3

class PageType(enum.Enum):
    """Page Type."""
    Normal = 1
    FrontMatter = 2
    BackMatter = 3
    Insert = 4
    Plate = 5

    @classmethod
    def page_type_from_separator(cls, separator:str):
        return {
            '.': cls.Normal,
            ',': cls.FrontMatter,
            ':': cls.BackMatter,
            'I': cls.Insert,
            'P': cls.Plate,
        }[separator]

class JournalVolume(Base, Timestamp):
    
    def __init__(self, type, journal, volume):
        self.type = type
        self.journal = journal
        self.volume = volume

    __tablename__ = 'journal_volume'
    __table_args__ = (Index('volume_index', "journal", "volume"), )

    id = Column(UUIDType, default=uuid.uuid4, primary_key=True)
    journal = Column(String)
    volume = Column(String)
    type = Column(String)
    status = Column(Enum(VolumeStatus))
    status_message = Column(String)
    file_hash = Column(String)

    UniqueConstraint(journal, volume)

    @classmethod
    def get_from_obj(cls, vol: JournalVolume, session: Session) -> JournalVolume:
        return session.query(cls).filter(cls.type == vol.type, cls.journal == vol.journal, cls.volume == vol.volume).one_or_none()
    
    @classmethod
    def get(cls, id: str, session: Session) -> JournalVolume:
        return session.query(cls).filter(cls.id == id).one_or_none()

    @classmethod
    def get_from_id_or_name(cls, id: str, session: Session) -> JournalVolume:
        vol = None
        if is_valid_uuid(id):
            vol = session.query(cls).filter(cls.id == id).one()
        if vol:
            return vol
        vol = session.query(cls).filter(cls.journal == id[0:5], cls.volume == id[5:9]).one()
        if vol:
            return vol
        raise ValueError
        

    @classmethod
    def get_errors(cls, session: Session) -> JournalVolume:
        return session.query(cls).filter(cls.status == VolumeStatus.Error).all()


page_article_association_table = Table('page2article', Base.metadata,
    Column('page_id', ForeignKey('page.id'), primary_key=True),
    Column('article_id', ForeignKey('article.id'), primary_key=True)
)

class Article(Base, Timestamp):
    __tablename__ = 'article'
    __table_args__ = (Index('article_volume_index', "journal_volume_id"), Index('article_bibcode_index', "bibcode"))

    def __init__(self, bibcode, journal_volume_id):
        self.bibcode = bibcode
        self.journal_volume_id = journal_volume_id

    id = Column(UUIDType, default=uuid.uuid4, primary_key=True)
    bibcode = Column(String, unique=True)
    journal_volume_id = Column(UUIDType, ForeignKey(JournalVolume.id))
    pages = relationship('Page', secondary=page_article_association_table, back_populates='articles', lazy='dynamic')

    @classmethod
    def get_or_create(cls, bibcode: str, journal_volume_id: uuid.UUID, session: Session) -> Article:
        article = session.query(cls).filter(cls.bibcode == bibcode, cls.journal_volume_id == journal_volume_id).one_or_none()
        if not article:
            article = Article(bibcode, journal_volume_id)
        return article

class Page(Base, Timestamp):
    __tablename__ = 'page'
    __table_args__ = (Index('page_volume_index', "journal_volume_id"), Index('page_name_index', "name"))

    def __init__(self, name, journal_volume_id):
        self.name = name
        self.journal_volume_id = journal_volume_id
        self.color_type = PageColor.BW
        self.parse_info_from_name(name)

    id = Column(UUIDType, default=uuid.uuid4,  primary_key=True)
    name = Column(String)
    label = Column(String)
    format = Column(String, default='image/tiff')
    color_type = Column(Enum(PageColor))
    page_type = Column(Enum(PageType))
    width = Column(Integer)
    height = Column(Integer)
    journal_volume_id = Column(UUIDType, ForeignKey(JournalVolume.id))
    volume_running_page_num = Column(Integer)
    articles = relationship('Article', secondary=page_article_association_table, back_populates='pages')

    UniqueConstraint(journal_volume_id, volume_running_page_num)
    UniqueConstraint(journal_volume_id, name)


    @classmethod
    def get_all_from_volume(cls, volume_id: uuid.UUID, session: Session) -> List[Page]:
        return session.query(cls).filter(cls.journal_volume_id == volume_id).all()
    
    @classmethod
    def get_from_name_and_journal(cls, name: str, volume_id: uuid.UUID, session: Session) -> Page:
        return session.query(cls).filter(cls.name == name, cls.journal_volume_id == volume_id).first()
    
    @classmethod
    def get_or_create(cls, name, journal_volume_id: uuid.UUID, session: Session) -> Page:
        page = cls.get_from_name_and_journal(name, journal_volume_id, session)
        if not page:
            page = Page(name, journal_volume_id)
        return page

    def parse_info_from_name(self, name) -> str:
        """In general file names follow the following format

        MNNNNNNSEEE

        The first position (M) is just 0 in most cases, but can be used as a modifier for specific cases (see the second table below).
        The next 6 positions are a number left padded with zeros. The 8th position is a separator; a period in many cases and other cases
        are explained in the first table below. The final 3 positions provide a means for further distinction; in many cases these are
        just 3 zeros, with special cases listed in the second table."""

        if len(name) != 11:
            raise PageNameException("Page name should consist of exactly 11 letters")
        
        self.page_type = PageType.page_type_from_separator(name[7])
        
        first_num = int(name[1:7])
        end_num = int(name[8:11])
        if end_num > 0:
            self.label = str(first_num) + "-" + str(end_num)
        else:
            self.label = str(first_num)