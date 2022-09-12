import re
import os
import html
from hashlib import md5
from typing import Iterable
from ADSScanExplorerPipeline.models import JournalVolume, Page, Article, PageColor, VolumeStatus, PageType
from ADSScanExplorerPipeline.exceptions import MissingImageFileException
import opensearchpy
from sqlalchemy.orm import Session
from PIL import Image
from PIL.TiffTags import TAGS
from adsputils import setup_logging, load_config
import boto3

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
    topmap_filepath = file_path + ".map"
    is_map_file = False
    if os.path.exists(topmap_filepath):
        file_path = topmap_filepath
        is_map_file = True
    running_page_num = 0
    with open(file_path) as file:
        for line in file:
            if is_map_file:
                page_name, label = split_top_map_row(line)
            else:
                page_name, label = split_top_row(line)
            if check_page_name_is_valid(page_name):
                running_page_num += 1
                page = Page.get_or_create(page_name, journal_volume.id, session)
                page.volume_running_page_num = running_page_num
                if label:
                    page.label = label
                yield page

def check_page_name_is_valid(page_name: str):
    if len(page_name) == 11:
        try:
            PageType.page_type_from_separator(page_name[7])
            return True
        except:
            return False
    return False

def split_top_map_row(line: str):
    """ Function to solit the lines in a.top.map file.
        These are tab separated and have the page type indication on the 3rd column instead of in the image name. 
        The image name is therefore adjusted to kepp in sync with the rest of the code
    """
    line_split = re.split(r"\s+", line)
    name = line_split[0]
    label = None
    if len(line_split) > 1:
        if not line_split[1].isspace() and len(line_split[1]) > 0:
            label = line_split[1]
    if len(line_split) > 2:
        type_index = 7
        type = line_split[2]
        if type == "1":
            name = name[:type_index] + "." + name[type_index + 1:]
        elif type == "C":
            name = name[:type_index] + "," + name[type_index + 1:]
        elif type == "B":
            name = name[:type_index] + ":" + name[type_index + 1:]
        elif type == "I":
            name = name[:type_index] + "I" + name[type_index + 1:]
        elif type == "P":
            name = name[:type_index] + "P" + name[type_index + 1:]
        elif type == "M":
            name = name[:type_index] + "M" + name[type_index + 1:]
    return name, label

def split_top_row(line: str):
    name = line[0:11]
    label = None
    page_label = line[11:].strip()
    if not page_label.isspace() and len(page_label) > 0:
        label = page_label
    return name, label


def parse_dat_file(file_path: str, journal_volume: JournalVolume, session: Session):
    """
    Loops through the volumes .dat file and yields a Article object for each row
    Each article gets linked with all pages associated to that article
    """
    with open(file_path) as file:
        line_num = 0
        for line in file:
            line_num += 1
            line_split = re.split(r"[\s+|]", line.strip())
            article_name = line_split[0]
            article = Article.get_or_create(article_name, journal_volume.id, session)
            article.pages = []
            first = True
            for page_name in line_split[3:]:
                if not check_page_name_is_valid(page_name):
                    continue
                page = Page.get_from_name_and_journal(page_name, journal_volume.id, session)
                if not page:
                    raise Exception("Page: " + page_name + " in .dat but not .top")
                if first:
                    article.start_page_number = page.volume_running_page_num
                    first = False
                if page not in article.pages:
                    article.pages.append(page)
            yield article

def check_all_image_files_exists(image_path: str, journal_volume: JournalVolume, session: Session):
    """
    Makes sure that all pages that have been found in the top file exists in the iamge folder as well
    """
    image_list = os.listdir(image_path)
    for page in Page.get_all_from_volume(journal_volume.id, session):
        if page.name not in image_list:
            raise MissingImageFileException("Missing image file %s", page.name)


def parse_image_files(image_path: str, journal_volume: JournalVolume, session: Session):
    """
    Loops through the volumes image files and parse out width and height from the TIFF header
    Some pages have multiple images a Black-and-White without file ending and a .tif which can
    be either grayscale or color based on the number of channels.
    """
    for filename in sorted(os.listdir(image_path)):
        try:
            if filename.endswith(".png") or filename.endswith(".jpg"):
                continue
            base_filename = filename.replace(".tif", "")
            page = Page.get_from_name_and_journal(base_filename, journal_volume.id, session)
            if not page:
                #Image file not in lists 
                #TODO possibly log this somewhere
                continue

            with Image.open(os.path.join(image_path, filename)) as img:
                meta_dict = {TAGS[key] : img.tag[key] for key in img.tag_v2}
                width = meta_dict["ImageWidth"][0]
                height = meta_dict["ImageLength"][0]

                if filename.endswith(".tif"):
                    n_samples = len(meta_dict["BitsPerSample"])
                    #The tiff images are either color if having 3 channels or grayscale if only 1 channel
                    if n_samples > 1:
                        color = PageColor.Color
                    else:
                        color = PageColor.Grayscale
                    page.color_type = color
                    page.width = width
                    page.height = height
                else:
                    page.width = width
                    page.height = height
            yield page
        except Exception as e:
            raise Exception("Failed to parse image file: " + os.path.join(image_path, filename) + " due to: " + str(e))

def upload_image_files(image_path: str, vol: JournalVolume, session: Session):
    """
    Uploads all image files which have been associated with a page in the volume to a s3 bucket defined in config
    """
    s3_bucket = boto3.resource("s3",\
        aws_access_key_id=config.get("S3_BUCKET_ACCESS_KEY", ""),\
        aws_secret_access_key=config.get("S3_BUCKET_SECRET_KEY", ""))\
            .Bucket(config.get('S3_BUCKET', ""))
    for filename in os.listdir(image_path):
        if filename.endswith(".png") or filename.endswith(".jpg"):
            continue
        base_filename = filename.replace(".tif", "")
        page = Page.get_from_name_and_journal(base_filename, vol.id, session)
        if not page:
            #Image file not in lists 
            continue
        file_path = os.path.join(image_path, filename)
        #TODO deal with 200dpi
        s3_file_path = os.path.join("bitmaps", vol.type, vol.journal.replace(".","_"), vol.volume, "600", filename)
        s3_bucket.upload_file(file_path, s3_file_path)

def index_ocr_files(ocr_path: str, vol: JournalVolume, session: Session):
    """
    Loops through all ocr files to the volume and adds them to an Open Search index.
    """

    opensearch = opensearchpy.OpenSearch(config.get("OPEN_SEARCH_URL", ""))
    query ={
        "query":{
            "term": {
                "volume_id": {
                    "value": vol.id
                }
             }
        }
    }
    opensearch.delete_by_query(index=config.get("OPEN_SEARCH_INDEX", ""), body=query)
    ocr_list = os.listdir(ocr_path)
    for page in Page.get_all_from_volume(vol.id, session):
        ocr_filename = page.name + ".txt"
        page_text = ''
        if ocr_filename not in ocr_list:
            logger.info("Missing ocr file " + page.name)
        else:
            with open(os.path.join(ocr_path, ocr_filename)) as file:    
                page_text = html.unescape(file.read())
        articles = []
        for article in page.articles:
            articles.append(article.bibcode)
        doc = {
            'page_id': page.id,
            'volume_id': vol.id,
            'text':  page_text,
            'article_bibcodes': articles,
            'journal': vol.journal,
            'volume': vol.volume,
            'volume_int': vol.volume,
            'page_type': page.page_type.name,
            'page_number': page.volume_running_page_num,
            'page_label': page.label,
            'page_color': page.color_type.name,
            'project': get_project_from_journal_name(page.journal_volume.journal)
        }
        opensearch.index(index=config.get("OPEN_SEARCH_INDEX", ""), body=doc)
        
def get_project_from_journal_name(journal_name:str):
    historical_journals = ['BuAst', 'OSUC.', 'BuChr', 'DurOO', 'POPot', 'GOAM.', 'PGenA', 'JBAA.', 'AnHar', 'ViHei', 'AnGVP', 'MiGoe', 'PA...', 'PSprO', 'MMAAR', 'VeLdn',
        'BuAsR', 'VeJen', 'MelAR', 'AnLL.', 'HarCi', 'PWasO', 'MiPul', 'DAOAR', 'BuAsI', 'BKUJ.', 'KVeBB', 'AnBog', 'OxfOO', 'PAICU', 'MadOO', 'LicOB', 'YerOB', 'AFChr', 
        'DunOP', 'AnBos', 'AnBor', 'VeBam', 'VeHei', 'CiUO.', 'RGOO.', 'POBol', 'MadOb', 'VeABD', 'PUSNO', 'MiSon', 'BTasO', 'VeBab', 'USNOY', 'Astr.', 'AnTou', 'ABSBe', 
        'PWHHO', 'USNOO', 'USNOM', 'MiBre', 'PAIKH', 'HarAR', 'AnEdi', 'AnMuS', 'OAORP', 'POslO', 'PUAms', 'KNAB.', 'WilOO', 'MelOO', 'AnAth', 'LowOB', 'HarRe', 'AnSWi', 
        'PCinO', 'VatRA', 'PTarO', 'AAHam', 'RAROC', 'CMWCI', 'MmBAA', 'VeMun', 'MiHam', 'PDDO.', 'POMil', 'MWOAR', 'AnCoi', 'AnBes', 'BUBes', 'USNOA', 'POMic', 'CoRut', 
        'AOTok', 'PLicO', 'ROCi.', 'PGooO', 'HelOB', 'TrTas', 'RAOU.', 'PCooO', 'AbbOO', 'YalRY', 'PYerO', 'RMROC', 'PTasO', 'GOAMM', 'YalOY', 'AnMun', 'AnOBN', 'BMOE.', 
        'TOYal', 'AnOSt', 'PGro.', 'AnLun', 'MiZur', 'HarOR', 'VeBB.', 'MNSSA', 'AnLei', 'PODE.', 'BSAFR', 'AnLuS', 'MmMtS', 'AnCap', 'HarPa', 'MtWAR', 'BSBA.', 'TsTas', 
        'AnSAO', 'VatPS', 'BESBe', 'BuBIH', 'AnWiD', 'MmSS.', 'VeLei', 'MmSSI', 'SidM.', 'PVasO', 'LAstr', 'PKUJ.', 'OSFOT', 'RNAO.', 'PDAO.', 'AnOB.', 'WinAR', 'AnWie', 
        'VeKAB', 'PMcCO', 'AnPOb', 'GORO.', 'TvOC.', 'SRMO.', 'RGAO.', 'TIUCS', 'PF+CO', 'LPlaS', 'BKAD.', 'IORA.', 'PRCO.', 'MNSSJ', 'CoPri', 'StoAn', 'POLyo', 'PPCAS', 
        'AnLow', 'AReg.', 'AnPar', 'CoLic', 'AnDea', 'PLPla', 'IORP.', 'CoKon', 'KodOB', 'YalAR', 'PDO..', 'AnNic', 'LawOB', 'TOMar', 'PCopO', 'VeBon', 'PLAGL', 'PAAS.', 
        'GVPOO', 'AnBru', 'CoMtW', 'VeGoe', 'MmArS', 'PKirO', 'AnRio', 'MmArc', 'MmEbr', 'AnStr', 'GazA.', 'AnOLL', 'ABMun', 'IORAS', 'BuLyo', 'SydOP']
    phaedra_journals = ['phae.']
    if journal_name in historical_journals:
        return 'Historical Literature'
    if journal_name in phaedra_journals:
        return 'PHaEDRA'
    return ''

def identify_journals(input_folder_path : str) -> Iterable[JournalVolume]:
    """
    Loops through the base folder to identify all journal volumnes that exists
    """
    list_path = os.path.join(input_folder_path, config.get('TOP_SUB_DIR', ''))
    for type in os.listdir(list_path):
        type_path = os.path.join(list_path, type)
        if not os.path.isdir(type_path):
            continue
        for journal in os.listdir(type_path):
            journal_path = os.path.join(type_path, journal) 
            if not os.path.isdir(journal_path):
                continue
            for file in os.listdir(journal_path):
                if file.endswith(".top"):
                    volume = parse_volume_from_top_file(file, journal)
                    vol = JournalVolume(type, journal, volume)
                    try:
                        vol.file_hash = hash_volume(input_folder_path, vol)
                    except Exception as e:
                        vol.status = VolumeStatus.Error
                        vol.status_message = "Error checking file hash on top file: " +  os.path.join(journal_path,file) + " due to " + str(e)
                        logger.error(vol.status_message)
                    yield vol

def parse_volume_from_top_file(filename : str, journal : str):
    """ Parses out the volume name from the top file"""
    return filename.replace(".top", "").replace(journal, "")

def hash_volume(base_path: str, vol: JournalVolume) -> str:
    """
    Calculates a md5 hash from the change dates and name of all the associated images and files to the volume  
    """
    vol_hash = ""
    list_path = os.path.join(base_path, config.get('TOP_SUB_DIR', '') ,vol.type, vol.journal)
    for file in sorted(os.listdir(list_path)):
        if str(vol.volume) in file:
            file_path = os.path.join(list_path, file)
            modified_time = os.path.getmtime(file_path)
            vol_hash = md5((vol_hash + str(modified_time) + file).encode("utf-8")).hexdigest()
    
    image_path = os.path.join(base_path, config.get('BITMAP_SUB_DIR', '') ,vol.type, vol.journal, vol.volume, "600")
    for file in sorted(os.listdir(image_path)):
        file_path = os.path.join(image_path, file)
        modified_time = os.path.getmtime(file_path)
        vol_hash = md5((vol_hash + str(modified_time) + file).encode("utf-8")).hexdigest()

    ocr_path = os.path.join(base_path, config.get('OCR_SUB_DIR', '') ,vol.type, vol.journal, vol.volume)
    for file in sorted(os.listdir(ocr_path)):
        file_path = os.path.join(ocr_path, file)
        modified_time = os.path.getmtime(file_path)
        vol_hash = md5((vol_hash + str(modified_time) + file).encode("utf-8")).hexdigest()
        
    return vol_hash

def set_ingestion_error_status(session: Session, journal_volume_id: str, error_msg: str):
    """
    Sets error status and error message on the failed volume
    """
    try:
        journal_volume = JournalVolume.get_from_id_or_name(journal_volume_id, session)
        journal_volume.status = VolumeStatus.Error
        journal_volume.status_message = error_msg
        session.add(journal_volume)
        session.commit()
    except Exception as e:
        logger.error("Failed setting error on volume: %s due to: %s", str(journal_volume_id), e)

def set_correct_volume_status(vol: JournalVolume, session: Session):
    if vol.status != VolumeStatus.Error:
        vol.status_message = ""
        if vol.bucket_uploaded and vol.db_done and vol.db_uploaded and vol.ocr_uploaded:
            vol.status = VolumeStatus.Done
        session.add(vol)
        session.commit()