from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, ForeignKey, Integer, String, Table, UniqueConstraint, Enum
from sqlalchemy.orm import relationship
from sqlalchemy_utils.types import  UUIDType
from ADSScanExplorerPipeline.exceptions import PageNameException
import uuid
import enum

Base = declarative_base()


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

class JournalVolume(Base):
    _tablename_ = 'journal_volume'

    id = Column(UUIDType, default=uuid.uuid4, primary_key=True)
    journal = Column(String)
    volume = Column(String)
    type = Column(String)


page_article_association_table = Table('page2article', Base.metadata,
    Column('page_id', ForeignKey('page.id'), primary_key=True),
    Column('article_id', ForeignKey('article.id'), primary_key=True)
)

class Article(Base):
    _tablename_ = 'article'

    id = Column(UUIDType, default=uuid.uuid4, primary_key=True)
    bibcode = Column(String)
    journal_volume_id = Column(UUIDType, ForeignKey(JournalVolume.id))
    pages = relationship('Page', secondary=page_article_association_table, back_populates='articles', lazy='dynamic')

class Page(Base):
    _tablename_ = 'page'

    def __init__(self, name):
        self.color_type = PageColor.BW
        self.parse_info_from_name(name)


    id = Column(UUIDType, default=uuid.uuid4, primary_key=True)
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

    @classmethod
    def get_from_name_and_journal(cls, name: str, journal_id:uuid, session):
        return session.query(cls).filter(cls.name == name, cls.journal_volume_id == journal_id).one_or_none()
    
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
            self.page_num = str(first_num) + "-" + str(end_num)
        else:
            self.page_num = str(first_num)