import re
import os
from hashlib import md5
from typing import Iterable
from ADSScanExplorerPipeline.models import JournalVolume, Page, Article, PageColor
from ADSScanExplorerPipeline.app import ADSScanExplorerPipeline
from sqlalchemy.orm import Session
from PIL import Image
from PIL.TiffTags import TAGS
from adsputils import setup_logging, load_config

# ============================= INITIALIZATION ==================================== #
# - Use app logger:
#import logging
#logger = logging.getLogger('ads-citation-capture')
# - Or individual logger for this file:
proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
config = load_config(proj_home=proj_home)
logger = setup_logging(__name__, proj_home=proj_home,
                        level=config.get('LOGGING_LEVEL', 'INFO'),
                        attach_stdout=config.get('LOG_STDOUT', False))


# =============================== FUNCTIONS ======================================= #
def parse_top_file(file_path: str, journal_volume: JournalVolume, session: Session) -> Iterable[Page]:
    """
    Loops through the volumes .top file and yields a Page object for each row
    """
    with open(file_path) as file:
        line_num = 0
        for line in file:
            line_num += 1
            if line_num < 5: #Top file contains 4 header lines
                continue
            running_page_num = line_num - 4
            line_split = re.split("\s+", line)
            #First is page name, second possibly page number if exists
            name = line_split[0]
            page = Page.get_or_create(name, journal_volume, session)
            page.volume_running_page_num = running_page_num
            if len(line_split) > 1:
                page_num = line_split[1]
                page.label = page_num
            yield page

def parse_dat_file(file_path: str, journal_volume: JournalVolume, session: Session):
    """
    Loops through the volumes .dat file and yields a Article object for each row
    Each article gets linked with all pages associated to that article
    """
    with open(file_path) as file:
        line_num = 0
        for line in file:
            line_num += 1
            line_split = re.split("[\s+|]", line.strip())
            article_name = line_split[0]
            article = Article.get_or_create(article_name, journal_volume, session)
            article_page_num = 0
            first = True
            for page_name in line_split[3:]:
                if len(page_name) != 11:
                    continue
                article_page_num += 1
                page = Page.get_from_name_and_journal(page_name, journal_volume.id, session)
                if first:
                    article.page_start = page.volume_running_page_num
                    first = False
                article.pages.append(page)
            article.page_end = page.volume_running_page_num
            yield article

def parse_image_files(image_path: str, journal_volume: JournalVolume, session: Session):
    """
    Loops through the volumes image files and parse out width and height from the TIFF header
    Some pages have multiple images a Black-and-White without file ending and a .tif which can
    be either greyscale or color based on the number of channels.
    """
    for filename in os.listdir(image_path):
        if filename.endswith(".png") or filename.endswith(".jpg"):
            continue
        base_filename = filename.replace(".tif", "")
        page = Page.get_from_name_and_journal(base_filename, journal_volume.id, session)
        if not page:
            #Image file not in lists 
            #TODO possibly log this somewhere
            continue

        img = Image.open(os.path.join(image_path, filename))
        meta_dict = {TAGS[key] : img.tag[key] for key in img.tag_v2}
        width = meta_dict["ImageWidth"][0]
        height = meta_dict["ImageLength"][0]

        if filename.endswith(".tif"):
            n_samples = len(meta_dict["BitsPerSample"])
            #The tiff images are either color if having 3 channels or greyscale if only 1 channel
            if n_samples > 1:
                color = PageColor.Color
            else:
                color = PageColor.Greyscale
            page.color_type = color
            page.width = width
            page.height = height
        else:
            page.width = width
            page.height = height
        img.close()
        yield page

def identify_journals(iput_folder_path : str) -> Iterable[JournalVolume]:
    """
    Loops through the base folder to identify all journal volumnes that exists
    """
    for type in os.listdir(iput_folder_path):
        type_path = os.path.join(iput_folder_path, type)
        if not os.path.isdir(type_path):
            continue
        for journal in os.listdir(type_path):
            journal_path = os.path.join(type_path, journal) 
            if not os.path.isdir(journal_path):
                continue
            for file in os.listdir(journal_path):
                if ".top" in file:
                    volume = parse_volume_from_top_file(file, journal)
                    vol = JournalVolume(type, journal, volume)
                    vol.file_hash = hash_volume()
                    yield vol

def parse_volume_from_top_file(filename : str, journal : str):
    """ Parses out the volume name from the top file"""
    return filename.replace(".top", "").replace(journal, "")

def hash_volume(base_path: str, vol: JournalVolume):
    """
    Calculates a md5 hash from the change dates of all the associated images and files to the volume  
    """
    vol_hash = ""
    list_path = os.path.join(base_path, "lists" ,vol.type, vol.journal)
    for file in os.listdir(list_path):
        file_path = os.path.join(list_path, file)
        modified_time = os.path.getmtime(file_path)
        vol_hash = md5((vol_hash + str(modified_time)).encode("utf-8")).hexdigest()
    
    image_path = os.path.join(base_path, "bitmaps" ,vol.type, vol.journal, vol.volume, "600")
    for file in os.listdir(image_path):
        file_path = os.path.join(image_path, file)
        modified_time = os.path.getmtime(file_path)
        vol_hash = md5((vol_hash + str(modified_time)).encode("utf-8")).hexdigest()
    return vol_hash