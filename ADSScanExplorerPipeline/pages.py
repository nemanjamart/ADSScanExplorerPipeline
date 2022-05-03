import re
import os
from ADSScanExplorerPipeline.models import JournalVolume, Page, Article, PageColor
from ADSScanExplorerPipeline.app import ADSScanExplorerPipeline
from sqlalchemy.orm import Session
from PIL import Image
from PIL.TiffTags import TAGS

def read_top_file(file_path: str, journal_volume: JournalVolume, session: Session):
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
            page = Page(name)

            page.journal_volume = journal_volume
            page.volume_running_page_num = running_page_num
            if len(line_split) > 1:
                page_num = line_split[1]
                page.label = page_num
            session.add(page)
            
def read_dat_file(file_path: str, journal_volume: JournalVolume, session: Session):
    with open(file_path) as file:
        line_num = 0
        for line in file:
            line_num += 1
            line_split = re.split("[\s+|]", line.strip())
            article_name = line_split[0]
            article = Article(article_name, journal_volume)
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
            session.add(article)

def read_image_files(image_path: str, journal_volume: JournalVolume, session: Session):
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
        session.add(page)
        img.close()