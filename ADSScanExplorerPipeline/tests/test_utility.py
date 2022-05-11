import unittest
from  ADSScanExplorerPipeline.utils import is_valid_uuid

class TestUtilities(unittest.TestCase):

    def test_uuid_validator(self):
        self.assertTrue(is_valid_uuid("ddb2497c-e9d8-4e08-a87b-466a723cba52"))
        self.assertFalse(is_valid_uuid("test"))
