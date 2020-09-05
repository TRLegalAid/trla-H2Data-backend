import unittest
from populate_database import geocode_manage_split
from add_housing import geocode_manage_split_housing
from colorama import Fore, Style
import os
import pandas as pd
import helpers
bad_accuracy_types = helpers.bad_accuracy_types
def assert_accuracies_and_inaccuracies(accurates, inaccurates):
    worksites_accurate = ((accurates["worksite accuracy"] >= 0.8) & (~accurates["worksite accuracy type"].isin(bad_accuracy_types))).all()
    accurates_h2a = accurates[accurates["Visa type"] == "H-2A"]
    housings_accurate = ((accurates_h2a["housing accuracy"] >= 0.8) & (~accurates_h2a["housing accuracy type"].isin(bad_accuracy_types))).all()
    inaccurates_h2a = inaccurates[inaccurates["Visa type"] == "H-2A"]
    inaccurate_conditions_h2a = ((inaccurates["housing accuracy"].isnull()) | (inaccurates["worksite accuracy"].isnull()) | (inaccurates["housing accuracy"] < 0.8) | (inaccurates["worksite accuracy"] < 0.8) | (inaccurates["housing accuracy type"].isin(bad_accuracy_types)) | (inaccurates["worksite accuracy type"].isin(bad_accuracy_types)))
    h2a_inaccurates_inaccurate = inaccurate_conditions_h2a.all()
    inaccurates_h2b = inaccurates[inaccurates["Visa type"] == "H-2B"]
    inaccurate_conditions_h2b = ((inaccurates["worksite accuracy"].isnull()) | (inaccurates["worksite accuracy"] < 0.8) | (inaccurates["worksite accuracy type"].isin(bad_accuracy_types)))
    h2b_inaccurates_inaccurate = inaccurate_conditions_h2b.all()
    return worksites_accurate, housings_accurate, h2a_inaccurates_inaccurate, h2b_inaccurates_inaccurate

# scraper_data = pd.read_excel(os.path.join(os.getcwd(), '..',  "excel_files/scraper_data.xlsx"))
# accurates, inaccurates, raw_scrapers = geocode_manage_split(scraper_data)
# class TestPopulateDatabase(unittest.TestCase):
#     def test_length_and_table_column(self):
#         self.assertEqual(len(accurates), 12)
#         self.assertEqual(len(inaccurates), 2)
#         self.assertEqual(len(raw_scrapers), 15)
#         self.assertTrue((accurates["table"] == "central").all() and (inaccurates["table"] == "central").all())
#     def test_accuracies(self):
#         worksites_accurate, housings_accurate, h2a_inaccurates_inaccurate, h2b_inaccurates_inaccurate = assert_accuracies_and_inaccuracies(accurates, inaccurates)
#         self.assertTrue(worksites_accurate)
#         self.assertTrue(housings_accurate)
#         self.assertTrue(h2a_inaccurates_inaccurate)
#         self.assertTrue(h2b_inaccurates_inaccurate)
#
#
# housing = pd.read_excel(os.path.join(os.getcwd(), '..', 'excel_files/housing_addendum.xlsx'))
# accurate_housing, inaccurate_housing = geocode_manage_split_housing(housing)
# class TestAddHousing(unittest.TestCase):
#     def test_length_and_table_column(self):
#         self.assertEqual(len(accurate_housing), 9)
#         self.assertEqual(len(inaccurate_housing), 1)
#         self.assertTrue((accurate_housing["table"] == "dol_h").all() and (inaccurate_housing["table"] == "dol_h").all())
#     def test_accuracies(self):
#         accurates_are_accurate = ((accurate_housing["housing accuracy"] >= 0.8) & (~(accurate_housing["housing accuracy type"].isin(bad_accuracy_types)))).all()
#         self.assertTrue(accurates_are_accurate)
#         inaccurates_are_inaccurate = ((inaccurate_housing["housing accuracy"].isnull()) | (inaccurate_housing["housing accuracy"] < 0.8) | (inaccurate_housing["housing accuracy type"].isin(bad_accuracy_types))).all()
#         self.assertTrue(inaccurates_are_inaccurate)


x = 4
class TestMergeDol(unittest.TestCase):
    def test_x_4(self):
        self.assertEqual(x, 4)
    def test_x_3(self):


        y = x - 1
        self.assertEqual(y, 3)






unittest.main(verbosity=2)
