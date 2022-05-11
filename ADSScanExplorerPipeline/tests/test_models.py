import unittest
from  ADSScanExplorerPipeline.models import PageType

class TestModels(unittest.TestCase):

    def testPageType(self):
        self.assertEqual(PageType.page_type_from_separator("."), PageType.Normal)
        self.assertEqual(PageType.page_type_from_separator(","), PageType.FrontMatter)
        self.assertEqual(PageType.page_type_from_separator(":"), PageType.BackMatter)
        self.assertEqual(PageType.page_type_from_separator("I"), PageType.Insert)
        self.assertEqual(PageType.page_type_from_separator("P"), PageType.Plate)