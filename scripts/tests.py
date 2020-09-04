import unittest
from populate_database import geocode_manage_split
from colorama import Fore, Style
import os
import pandas as pd
import helpers


def assert_accuracies_and_inaccuracies(accurates, inaccurates):
    worksites_accurate = (accurates["worksite accuracy"] >= 0.8).all()
    accurates_h2a = accurates[accurates["Visa type"] == "H-2A"]
    housings_accurate = (accurates_h2a["housing accuracy"] >= 0.8).all()
    inaccurates_h2a = inaccurates[inaccurates["Visa type"] == "H-2A"]
    inaccurate_conditions_h2a = ((inaccurates["housing accuracy"].isnull()) | (inaccurates["worksite accuracy"].isnull()) | (inaccurates["housing accuracy"] < 0.8) | (inaccurates["worksite accuracy"] < 0.8) | (inaccurates["housing accuracy type"].isin(helpers.bad_accuracy_types)) | (inaccurates["worksite accuracy type"].isin(helpers.bad_accuracy_types)))
    inaccurates_inaccurate = inaccurate_conditions_h2a.all()
    inaccurates_h2b = inaccurates[inaccurates["Visa type"] == "H-2B"]
    inaccurate_conditions_h2b = ((inaccurates["worksite accuracy"].isnull()) | (inaccurates["worksite accuracy"] < 0.8) | (inaccurates["worksite accuracy type"].isin(helpers.bad_accuracy_types)))
    h2b_inaccurates_inaccurate = inaccurate_conditions_h2b.all()
    return worksites_accurate, housings_accurate, inaccurates_inaccurate, h2b_inaccurates_inaccurate


scraper_data = pd.read_excel(os.path.join(os.getcwd(), '..',  "excel_files/scraper_data.xlsx"))
accurates, inaccurates, raw_scrapers = geocode_manage_split(scraper_data)
class TestPopulateDatabase(unittest.TestCase):

    def test_length_and_table_column(self):
        self.assertEqual(len(accurates), 12)
        self.assertEqual(len(inaccurates), 2)
        self.assertEqual(len(raw_scrapers), 15)
        self.assertTrue((accurates["table"] == "central").all() and (inaccurates["table"] == "central").all())

    def test_accuracies(self):
        self.assertFalse(all(assert_accuracies_and_inaccuracies(accurates, inaccurates)))

unittest.main()
