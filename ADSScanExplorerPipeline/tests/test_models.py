import unittest
from  ADSScanExplorerPipeline.models import PageType, Page
from  ADSScanExplorerPipeline.exceptions import PageNameException

class TestModels(unittest.TestCase):

    def testPageType(self):
        self.assertEqual(PageType.page_type_from_separator("."), PageType.Normal)
        self.assertEqual(PageType.page_type_from_separator(","), PageType.FrontMatter)
        self.assertEqual(PageType.page_type_from_separator(":"), PageType.BackMatter)
        self.assertEqual(PageType.page_type_from_separator("I"), PageType.Insert)
        self.assertEqual(PageType.page_type_from_separator("P"), PageType.Plate)
        self.assertEqual(PageType.page_type_from_separator("M"), PageType.Normal)
    
    def testPageLabel(self):
        page = Page("0000255.000", "vol_id")
        self.assertEqual(page.label, "255")

        page = Page("0000255.001", "vol_id")
        self.assertEqual(page.label, "255-1")

        page = Page("A000255.000", "vol_id")
        self.assertEqual(page.label, "A-255")
        
        page = Page("A000255.001", "vol_id")
        self.assertEqual(page.label, "A-255-1")

        self.assertRaises(PageNameException, Page, "00023232", "")
