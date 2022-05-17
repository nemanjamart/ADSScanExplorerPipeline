from concurrent.futures import process
import os
import unittest
from unittest.mock import patch, Mock
from alchemy_mock.mocking import UnifiedAlchemyMagicMock
from ADSScanExplorerPipeline.tasks import task_investigate_new_volumes, task_process_volume, task_upload_image_files_for_volume, task_index_ocr_files_for_volume
from ADSScanExplorerPipeline.models import JournalVolume, VolumeStatus, Page, PageColor, PageType, Article
from moto import mock_s3
import boto3

class TestModels(unittest.TestCase):

    test_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
    data_folder = os.path.join(test_home, "tests/data/")

    @patch('ADSScanExplorerPipeline.app.ADSScanExplorerPipeline.session_scope')
    def test_task_investigate_new_volumes(self, session_scope):
        session = UnifiedAlchemyMagicMock()
        session_scope.return_value = session
        used_session = task_investigate_new_volumes(self.data_folder, upload_files = False, process=False)
        self.assertEqual(len(used_session.query(JournalVolume).filter().all()), 1)
        for vol in used_session.query(JournalVolume).filter(JournalVolume.journal == "").all():
            self.assertEqual(vol.type, "seri")
            self.assertEqual(vol.journal, "test.")
            self.assertEqual(vol.volume, "0001")
            self.assertEqual(vol.status, VolumeStatus.New)
        
    @patch('ADSScanExplorerPipeline.app.ADSScanExplorerPipeline.session_scope')
    @patch('ADSScanExplorerPipeline.models.JournalVolume.get_from_id_or_name')
    def test_task_process_volume(self, get_from_id_or_name, session_scope):
        vol = JournalVolume("seri", "test.", "0001")
        vol.id = '60181735-6f0c-47a6-bf9d-47a1f1fc4fc4'
        get_from_id_or_name.return_value = vol

        expected_page =  Page("0000255,001", vol.id)
        expected_page.label = "255-01"
        expected_page.volume_running_page_num = 1
        expected_page.color_type = PageColor.Greyscale
        expected_page.page_type = PageType.FrontMatter
        
        expected_article =  Article("test......001..test", vol.id)

        session = UnifiedAlchemyMagicMock()
        session_scope.return_value = session

        used_session = task_process_volume(self.data_folder, vol.id)
        for vol in used_session.query(JournalVolume).filter(JournalVolume.journal == "").all():
            self.assertEqual(vol.type, "seri")
            self.assertEqual(vol.journal, "test.")
            self.assertEqual(vol.volume, "0001")
            self.assertEqual(vol.status, VolumeStatus.Db_done)

        #Mocked session doesn't update the row but adds a new row when adding to the db therefore we get 3 identical rows
        self.assertEqual(len(used_session.query(Page).filter().all()), 3)
        for page in used_session.query(Page).filter(JournalVolume.journal == "").all():
            self.assertEqual(page.name, expected_page.name)
            self.assertEqual(page.label, expected_page.label)
            self.assertEqual(page.journal_volume_id, vol.id)
            self.assertEqual(page.volume_running_page_num, expected_page.volume_running_page_num)
            self.assertEqual(page.color_type, expected_page.color_type)
            self.assertEqual(page.page_type, expected_page.page_type)
            self.assertEqual(len(page.articles), 1)
            self.assertEqual(page.articles[0].bibcode, expected_article.bibcode)

        self.assertEqual(len(used_session.query(Article).filter().all()), 1)
        for article in used_session.query(Article).filter(JournalVolume.journal == "").all():
            self.assertEqual(article.bibcode, expected_article.bibcode)
            self.assertEqual(article.journal_volume_id, vol.id)

    @mock_s3
    @patch('ADSScanExplorerPipeline.app.ADSScanExplorerPipeline.session_scope')
    @patch('ADSScanExplorerPipeline.models.JournalVolume.get_from_id_or_name')
    @patch('ADSScanExplorerPipeline.models.Page.get_all_from_volume')
    @patch('ADSScanExplorerPipeline.models.Page.get_from_name_and_journal')
    def test_task_upload_image_files_for_volume(self, get_from_name_and_journal, get_all_from_volume, get_from_id_or_name, session_scope):
        
        vol = JournalVolume("seri", "test.", "0001")
        vol.id = '60181735-6f0c-47a6-bf9d-47a1f1fc4fc4'
        get_from_id_or_name.return_value = vol

        expected_page =  Page("0000255,001", vol.id)
        get_all_from_volume.return_value = [expected_page]
        get_from_name_and_journal.return_value = expected_page

        session = UnifiedAlchemyMagicMock()
        session_scope.return_value = session

        conn = boto3.resource('s3')
        bucket = conn.create_bucket(Bucket='scan-explorer')

        used_session = task_upload_image_files_for_volume(self.data_folder, vol.id)

        for vol in used_session.query(JournalVolume).filter(JournalVolume.journal == "").all():
            self.assertEqual(vol.status, VolumeStatus.Bucket_done)

        keys = []
        for obj in bucket.objects.all():
            keys.append(obj.key)
        self.assertEqual(len(keys), 2)
        self.assertTrue('bitmaps/seri/test_/0001/600/0000255,001' in keys)
        self.assertTrue('bitmaps/seri/test_/0001/600/0000255,001.tif' in keys)

    @patch('ADSScanExplorerPipeline.app.ADSScanExplorerPipeline.session_scope')
    @patch('ADSScanExplorerPipeline.models.JournalVolume.get_from_id_or_name')
    @patch('ADSScanExplorerPipeline.models.Page.get_all_from_volume')
    @patch('elasticsearch.Elasticsearch')
    def test_task_index_ocr_files_for_volume(self, Elasticsearch, get_all_from_volume, get_from_id_or_name, session_scope):
        vol = JournalVolume("seri", "test.", "0001")
        vol.id = '60181735-6f0c-47a6-bf9d-47a1f1fc4fc4'
        get_from_id_or_name.return_value = vol
        
        session = UnifiedAlchemyMagicMock()
        session_scope.return_value = session

        expected_page =  Page("0000255,001", vol.id)
        expected_page.id = "7385f212-d403-46bd-b235-96d6da01ce0c"
        get_all_from_volume.return_value = [expected_page]

        used_session = task_index_ocr_files_for_volume(self.data_folder, vol.id)
        for vol in used_session.query(JournalVolume).filter(JournalVolume.journal == "").all():
            self.assertEqual(vol.status, VolumeStatus.Done)
        
        Elasticsearch.assert_called()
        Elasticsearch.return_value.delete_by_query.assert_called()
        self.assertEqual("{'page_id': '7385f212-d403-46bd-b235-96d6da01ce0c', 'volume_id': '60181735-6f0c-47a6-bf9d-47a1f1fc4fc4', 'text': 'test ocr text', 'articles': []}" , str(Elasticsearch.return_value.index.call_args_list[0][1]['document']))
