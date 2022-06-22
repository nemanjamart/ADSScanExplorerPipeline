from concurrent.futures import process
import os
import unittest
from unittest.mock import patch, MagicMock
from alchemy_mock.mocking import UnifiedAlchemyMagicMock
from ADSScanExplorerPipeline.tasks import task_process_new_volumes, task_process_volume, task_upload_image_files_for_volume, task_index_ocr_files_for_volume
from ADSScanExplorerPipeline.models import JournalVolume, VolumeStatus, Page, PageColor, PageType, Article
from moto import mock_s3
import boto3

class TestModels(unittest.TestCase):

    test_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
    data_folder = os.path.join(test_home, "tests/data/")

    @patch('ADSScanExplorerPipeline.app.ADSScanExplorerPipeline.session_scope')
    def test_task_process_new_volumes(self, session_scope):
        session = UnifiedAlchemyMagicMock()
        session_scope.return_value = session
        used_session = task_process_new_volumes(self.data_folder, upload_files = False, process=False)
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
        #Preset these to done to get Done status in the end
        vol.bucket_uploaded = True
        vol.db_uploaded = True
        vol.ocr_uploaded = True
        get_from_id_or_name.return_value = vol

        expected_page =  Page("0000255,001", vol.id)
        expected_page.label = "255-01"
        expected_page.volume_running_page_num = 1
        expected_page.color_type = PageColor.Greyscale
        expected_page.page_type = PageType.FrontMatter
        
        expected_article =  Article("test......001..test", vol.id)

        session = UnifiedAlchemyMagicMock()
        session_scope.return_value = session

        used_session = task_process_volume(self.data_folder, vol.id, upload_files=False, index_ocr=False, upload_db=False)
        for vol in used_session.query(JournalVolume).filter(JournalVolume.journal == "").all():
            self.assertEqual(vol.type, "seri")
            self.assertEqual(vol.journal, "test.")
            self.assertEqual(vol.volume, "0001")
            self.assertTrue(vol.db_done)
            self.assertEqual(vol.status, VolumeStatus.Done)
            expected_dict = {
                'type': 'seri',
                'journal': 'test.',
                'volume': '0001',
                'pages':[{
                    'name': '0000255,001',
                    'label': '255-01',
                    'format': 'image/tiff',
                    'color_type': 'Greyscale',
                    'page_type': 'FrontMatter',
                    'width': 3904,
                    'height': 5312,
                    'volume_running_page_num': 1,
                    'articles': [{'bibcode':'test......001..test'}] 
                    }]
            }
            self.assertEqual(vol.to_dict(), expected_dict)


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
            self.assertTrue(vol.bucket_uploaded)

        keys = []
        for obj in bucket.objects.all():
            keys.append(obj.key)
        self.assertEqual(len(keys), 2)
        self.assertTrue('bitmaps/seri/test_/0001/600/0000255,001' in keys)
        self.assertTrue('bitmaps/seri/test_/0001/600/0000255,001.tif' in keys)

    @patch('ADSScanExplorerPipeline.app.ADSScanExplorerPipeline.session_scope')
    @patch('ADSScanExplorerPipeline.models.JournalVolume.get_from_id_or_name')
    @patch('ADSScanExplorerPipeline.models.Page.get_all_from_volume')
    @patch('opensearchpy.OpenSearch')
    def test_task_index_ocr_files_for_volume(self, OpenSearch, get_all_from_volume, get_from_id_or_name, session_scope):
        vol = JournalVolume("seri", "test.", "0001")
        get_from_id_or_name.return_value = vol
        
        session = UnifiedAlchemyMagicMock()
        session_scope.return_value = session

        expected_page =  Page("0000255,001", vol.id)
        get_all_from_volume.return_value = [expected_page]

        used_session = task_index_ocr_files_for_volume(self.data_folder, vol.id)
        for vol in used_session.query(JournalVolume).filter(JournalVolume.journal == "").all():
            self.assertTrue(vol.ocr_uploaded)
        
        OpenSearch.assert_called()
        OpenSearch.return_value.delete_by_query.assert_called()
        self.assertEqual("{'page_id': 'test.0001_0000255,001', 'volume_id': 'test.0001', 'text': 'test ocr text', 'article_ids': []}" , str(OpenSearch.return_value.index.call_args_list[0][1]['body']))

    @patch('ADSScanExplorerPipeline.app.ADSScanExplorerPipeline.session_scope')
    @patch('ADSScanExplorerPipeline.models.JournalVolume.get_from_id_or_name')
    @patch('requests.put')
    def test_task_task_upload_db_for_volume(self, mock_put, get_from_id_or_name, session_scope):
        vol = JournalVolume("seri", "test.", "0001")
        get_from_id_or_name.return_value = vol

        session = UnifiedAlchemyMagicMock()
        session_scope.return_value = session
       
        mock_return = MagicMock()
        mock_return.status_code = 200
        mock_put.return_value = mock_return

        used_session = task_process_volume(self.data_folder, vol.id, upload_files=False, index_ocr=False, upload_db=True)
        
        expected_request_args = {'type': 'seri', 'journal': 'test.', 'volume': '0001', 'pages': [{'name': '0000255,001', 'label': '255-01', 'format': 'image/tiff', 'color_type': 'Greyscale', 'page_type': 'FrontMatter', 'width': 3904, 'height': 5312, 'volume_running_page_num': 1, 'articles': [{'bibcode': 'test......001..test'}]}]}
        from adsputils import load_config
        proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../../'))
        config = load_config(proj_home=proj_home)
        url = config.get('SERVICE_DB_PUSH_URL' ,'')
        mock_put.assert_called()    
        mock_put.assert_called_with(url, json=expected_request_args )

        for vol in used_session.query(JournalVolume).filter(JournalVolume.journal == "").all():
            print(vol)
            self.assertEqual(vol.type, "seri")
            self.assertEqual(vol.journal, "test.")
            self.assertEqual(vol.volume, "0001")
            self.assertEqual(vol.status, VolumeStatus.Processing)