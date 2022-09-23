import unittest
from unittest.mock import patch
from alchemy_mock.mocking import UnifiedAlchemyMagicMock
import os
from ADSScanExplorerPipeline.models import JournalVolume, Page, Article, PageColor
from ADSScanExplorerPipeline.exceptions import MissingImageFileException
from ADSScanExplorerPipeline.ingestor import hash_volume, identify_journals, parse_volume_from_top_file, parse_top_file, parse_dat_file, parse_image_files, check_all_image_files_exists, upload_image_files, split_top_row, split_top_map_row
from moto import mock_s3
import boto3

class TestIngestor(unittest.TestCase):

    test_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
    data_folder = os.path.join(test_home, "tests/data/")

    def test_vol_hash(self):
        vol = JournalVolume("seri", "test.", "0001")
        hash = hash_volume(self.data_folder, vol)
        #Hash changes in different OS env due to full relative link being different
        #self.assertEqual(hash, "15716c95d1241e876efc339e98fa206e")
    
    def test_parse_volume(self):
        vol = JournalVolume("seri", "test.", "0001")
        volym_str = parse_volume_from_top_file("test.0001.top", vol.journal)
        self.assertEqual(volym_str, "0001")

    def test_identify_journals(self):
        expected_vol = JournalVolume("seri", "test.", "0001")
        for vol in identify_journals(self.data_folder):
            self.assertEqual(vol.type, expected_vol.type)
            self.assertEqual(vol.journal, expected_vol.journal)
            self.assertEqual(vol.volume, expected_vol.volume)
    
    def test_parse_top_row(self):
        name, label = split_top_row("A000136.000")
        self.assertEqual(name, "A000136.000")
        self.assertEqual(label, None)
        
        name, label = split_top_row("A000136.000       A")
        self.assertEqual(name, "A000136.000")
        self.assertEqual(label, "A")
        
        name, label = split_top_row("A000136.000A261/A262")
        self.assertEqual(name, "A000136.000")
        self.assertEqual(label, "A261/A262")
        
        name, label = split_top_row("A000136.000261/262")
        self.assertEqual(name, "A000136.000")
        self.assertEqual(label, "261/262")

    def test_parse_top_map_row(self):
        name, label = split_top_map_row("A000136.000 ii 1")
        self.assertEqual(name, "A000136.000")
        self.assertEqual(label, "ii")
        
        name, label = split_top_map_row("A000136.000 A261/A262 I")
        self.assertEqual(name, "A000136I000")
        self.assertEqual(label, "A261/A262")

        name, label = split_top_map_row("A000136.000 A261/A262 B")
        self.assertEqual(name, "A000136:000")
        self.assertEqual(label, "A261/A262")
        
        name, label = split_top_map_row("A000136.000 A261/A262 C")
        self.assertEqual(name, "A000136,000")
        self.assertEqual(label, "A261/A262")

        name, label = split_top_map_row("A000136.000 A261/A262 P")
        self.assertEqual(name, "A000136P000")
        self.assertEqual(label, "A261/A262")
        
        name, label = split_top_map_row("A000136.000 A261/A262 M")
        self.assertEqual(name, "A000136M000")
        self.assertEqual(label, "A261/A262")

    @patch('sqlalchemy.orm.Session')
    def test_parse_top_file(self, Session):
        session = Session.return_value
        session.query.return_value.filter.return_value.first.return_value = None
        vol = JournalVolume("seri", "test.", "0001")
        vol.id = '60181735-6f0c-47a6-bf9d-47a1f1fc4fc4'
        expected_page =  Page("0000255,001", vol.id)
        expected_page.label =  "255-01"
        top_filename = vol.journal + vol.volume + ".top"
        top_file_path = os.path.join(self.data_folder, "lists", vol.type, vol.journal, top_filename)
        n = 0
        for page in parse_top_file(top_file_path, vol, session):
            n += 1
            self.assertEqual(page.name, expected_page.name)
            self.assertEqual(page.journal_volume_id, expected_page.journal_volume_id)
            self.assertEqual(page.label, expected_page.label)
        self.assertEqual(n , 1)

    @patch('sqlalchemy.orm.Session')
    @patch('ADSScanExplorerPipeline.models.Page.get_from_name_and_journal')
    def test_parse_dat_file(self, get_from_name_and_journal, Session):
        session = Session.return_value
        session.query.return_value.filter.return_value.one_or_none.return_value = None
        vol = JournalVolume("seri", "test.", "0001")
        page =  Page("0000255,001", vol.id)
        expected_article =  Article("test......001..test", vol.id)
        get_from_name_and_journal.return_value = page
        dat_filename = vol.journal + vol.volume + ".dat"
        dat_file_path = os.path.join(self.data_folder, "lists", vol.type, vol.journal, dat_filename)
        n = 0
        for article in parse_dat_file(dat_file_path, vol, session):
            n += 1
            self.assertEqual(article.bibcode, expected_article.bibcode)
            self.assertTrue(page in article.pages)
        self.assertEqual(n , 1)

    @patch('ADSScanExplorerPipeline.models.Page.get_from_name_and_journal')
    def test_parse_image_files(self, get_from_name_and_journal):
        vol = JournalVolume("seri", "test.", "0001")
        expected_page =  Page("0000255,001", vol.id)
        get_from_name_and_journal.return_value = expected_page
        image_folder_path = os.path.join(self.data_folder,  "bitmaps", vol.type, vol.journal, vol.volume, "600")
        n = 0
        for page in parse_image_files(image_folder_path, vol, None):
            n += 1
            self.assertEqual(page.name, expected_page.name)
            self.assertEqual(page.height,5312)
            self.assertTrue(page.width, 4320)
            self.assertTrue(page.color_type in [PageColor.Grayscale, PageColor.BW])
        self.assertEqual(n , 2)

    @patch('ADSScanExplorerPipeline.models.Page.get_from_name_and_journal')
    def test_parse_image_files_wrong_page(self, get_from_name_and_journal):
        vol = JournalVolume("seri", "test.", "0001")
        get_from_name_and_journal.return_value = None
        image_folder_path = os.path.join(self.data_folder,  "bitmaps", vol.type, vol.journal, vol.volume, "600")
        for page in parse_image_files(image_folder_path, vol, None):
            raise ValueError("Should not be here")

    @patch('ADSScanExplorerPipeline.models.Page.get_all_from_volume')
    def test_parse_image_files_not_missing_page(self, get_all_from_volume):
        vol = JournalVolume("seri", "test.", "0001")
        expected_page =  Page("0000255,001", vol.id)
        get_all_from_volume.return_value = [expected_page]
        image_folder_path = os.path.join(self.data_folder,  "bitmaps", vol.type, vol.journal, vol.volume, "600")
        check_all_image_files_exists(image_folder_path, vol, None)

    @patch('ADSScanExplorerPipeline.models.Page.get_all_from_volume')
    def test_parse_image_files_missing_page(self, get_all_from_volume):
        vol = JournalVolume("seri", "test.", "0001")
        expected_page =  Page("0000256,001", vol.id)
        get_all_from_volume.return_value = [expected_page]
        image_folder_path = os.path.join(self.data_folder,  "bitmaps", vol.type, vol.journal, vol.volume, "600")
        self.assertRaises(MissingImageFileException, check_all_image_files_exists, image_folder_path, vol, None)

    @mock_s3
    @patch('ADSScanExplorerPipeline.models.Page.get_from_name_and_journal')
    def test_upload_images(self, get_from_name_and_journal):
        """ Makes sure the files are uploaded to a mock s3 bucket"""
        vol = JournalVolume("seri", "test.", "0001")
        image_folder_path = os.path.join(self.data_folder, "bitmaps", vol.type, vol.journal, vol.volume, "600")
        expected_page =  Page("0000255,001", vol.id)
        get_from_name_and_journal.return_value = expected_page
        conn = boto3.resource('s3')
        bucket = conn.create_bucket(Bucket='scan-explorer')
        upload_image_files(image_folder_path, vol, None)
        keys = []
        for obj in bucket.objects.all():
            keys.append(obj.key)
        self.assertEqual(len(keys), 2)
        self.assertTrue('bitmaps/seri/test_/0001/600/0000255,001' in keys)
        self.assertTrue('bitmaps/seri/test_/0001/600/0000255,001.tif' in keys)
    
    def test_parse_problematic_files(self):
        session = UnifiedAlchemyMagicMock()
        vol = JournalVolume("seri", "test.", "0002")
        top_filename = vol.journal + vol.volume + ".top"
        dat_filename = vol.journal + vol.volume + ".dat"
        top_file_path = os.path.join(self.data_folder, "problematic_lists", top_filename)
        dat_file_path = os.path.join(self.data_folder, "problematic_lists", dat_filename)

        for page in parse_top_file(top_file_path, vol, session):
            session.add(page)
        for article in parse_dat_file(dat_file_path, vol, session):
            session.add(article)

        self.assertEqual(session.query(Page).count(),5)
        self.assertEqual(session.query(Article).count(),2)
