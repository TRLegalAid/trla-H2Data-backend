import unittest
from populate_database import geocode_manage_split
from add_housing import geocode_manage_split_housing
from merge_dol import geocode_manage_split_merge
from helpers import merge_all_data, get_value, myprint
from colorama import Fore, Style
import os
import pandas as pd
import numpy as np
import helpers
bad_accuracy_types = helpers.bad_accuracy_types
def assert_accuracies_and_inaccuracies(accurates, inaccurates):
    worksites_accurate = ((accurates["worksite accuracy"] >= 0.8) & (~accurates["worksite accuracy type"].isin(bad_accuracy_types))).all()
    accurates_h2a = accurates[accurates["Visa type"] == "H-2A"]
    housings_accurate = ((accurates_h2a["housing accuracy"] >= 0.8) & (~accurates_h2a["housing accuracy type"].isin(bad_accuracy_types))).all()
    inaccurates_h2a = inaccurates[inaccurates["Visa type"] == "H-2A"]
    inaccurate_conditions_h2a = ((inaccurates_h2a["housing accuracy"].isnull()) | (inaccurates_h2a["worksite accuracy"].isnull()) | (inaccurates_h2a["housing accuracy"] < 0.8) | (inaccurates_h2a["worksite accuracy"] < 0.8) | (inaccurates_h2a["housing accuracy type"].isin(bad_accuracy_types)) | (inaccurates_h2a["worksite accuracy type"].isin(bad_accuracy_types)))
    h2a_inaccurates_inaccurate = inaccurate_conditions_h2a.all()
    inaccurates_h2b = inaccurates[inaccurates["Visa type"] == "H-2B"]
    if len(inaccurates_h2b) != 0:
        inaccurate_conditions_h2b = ((inaccurates["worksite accuracy"].isnull()) | (inaccurates_h2b["worksite accuracy"] < 0.8) | (inaccurates_h2b["worksite accuracy type"].isin(bad_accuracy_types)))
        h2b_inaccurates_inaccurate = inaccurate_conditions_h2b.all()
    else:
        h2b_inaccurates_inaccurate = True
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
# #
# #
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


# old_accurates = pd.read_excel(os.path.join(os.getcwd(), '..', 'excel_files/accurates_geocoded.xlsx'))
# old_inaccurates = pd.read_excel(os.path.join(os.getcwd(), '..', 'excel_files/inaccurates_geocoded.xlsx'))
# dol_jobs = pd.read_excel(os.path.join(os.getcwd(), '..', 'excel_files/dol_data.xlsx'))
# accurate_jobs_merged, inaccurate_jobs_merged = geocode_manage_split_merge(dol_jobs, old_accurates, old_inaccurates)
# class TestMergeDol(unittest.TestCase):
#     def test_length_and_source_column(self):
#         self.assertEqual(len(accurate_jobs_merged), 18)
#         self.assertEqual(len(inaccurate_jobs_merged), 9)
#     def test_accuracies(self):
#         worksites_accurate, housings_accurate, h2a_inaccurates_inaccurate, h2b_inaccurates_inaccurate = assert_accuracies_and_inaccuracies(accurate_jobs_merged, inaccurate_jobs_merged)
#         self.assertTrue(worksites_accurate)
#         self.assertTrue(housings_accurate)
#         self.assertTrue(h2a_inaccurates_inaccurate)
#         self.assertTrue(h2b_inaccurates_inaccurate)

class TestImplementFixes(unittest.TestCase):
    pass

class TestUpdateDatabase(unittest.TestCase):
    pass


# TESTING MERGE ALL DATA FUNCTION - see project documentation to see what is meant by case_1 through case_8
accurate_new_jobs = pd.read_excel(os.path.join(os.getcwd(), '..',  "excel_files/accurate_dol_geocoded.xlsx"), converters={'fixed': bool, 'housing_fixed_by': str, 'worksite_fixed_by': str})
inaccurate_new_jobs = pd.read_excel(os.path.join(os.getcwd(), '..',  "excel_files/inaccurate_dol_geocoded.xlsx"),  converters={'fixed': bool, 'housing_fixed_by': str, 'worksite_fixed_by': str})
accurate_old_jobs = pd.read_excel(os.path.join(os.getcwd(), '..',  "excel_files/accurates_geocoded.xlsx"), converters={'fixed': bool, 'housing_fixed_by': str, 'worksite_fixed_by': str})
inaccurate_old_jobs = pd.read_excel(os.path.join(os.getcwd(), '..',  "excel_files/inaccurates_geocoded.xlsx"),  converters={'fixed': bool, 'housing_fixed_by': str, 'worksite_fixed_by': str})

def has_no_dups(accurates, inaccurates):
    accurate_case_numbers = accurates["CASE_NUMBER"].tolist()
    accs_no_dups = (len(accurate_case_numbers) == len(set(accurate_case_numbers)))
    inaccurate_case_numbers = inaccurates["CASE_NUMBER"].tolist()
    inaccs_no_dups = (len(inaccurate_case_numbers) == len(set(inaccurate_case_numbers)))
    all_case_numbers = accurate_case_numbers + inaccurate_case_numbers
    no_dups_anywhere = len(all_case_numbers) == len(set(all_case_numbers))
    return accs_no_dups, inaccs_no_dups, no_dups_anywhere

all_accurate_jobs, all_inaccurate_jobs = merge_all_data(accurate_new_jobs.copy(), inaccurate_new_jobs.copy(), accurate_old_jobs.copy(), inaccurate_old_jobs.copy())
class TestCaseZero(unittest.TestCase):
    def test_initital_case(self):
        self.assertEqual(len(all_accurate_jobs), 19)
        self.assertEqual(len(all_inaccurate_jobs), 9)
        self.assertTrue(all(has_no_dups(all_accurate_jobs, all_inaccurate_jobs)))

acc_new1 = accurate_new_jobs.copy()
acc_new1_len = len(acc_new1)
acc_new1.at[acc_new1_len - 1, "CASE_NUMBER"] = "H-300-20119-524313"
acc_new1.at[acc_new1_len - 2, "CASE_NUMBER"] = "H-300-20125-538913"
all_accs1, all_inaccs1 = merge_all_data(acc_new1, inaccurate_new_jobs.copy(), accurate_old_jobs.copy(), inaccurate_old_jobs.copy())
all_accs1_len, all_inaccs1_len = len(all_accs1), len(all_inaccs1)
class TestCaseOne(unittest.TestCase):
    def test_lengths(self):
        self.assertEqual(all_accs1_len, 17)
        self.assertEqual(all_inaccs1_len, 9)
    def test_no_dups(self):
        self.assertTrue(all(has_no_dups(all_accs1, all_inaccs1)))
    def test_used_right_data(self):
        dup_job = all_accs1[all_accs1["CASE_NUMBER"] == "H-300-20125-538913"]
        # testing columns from dol
        self.assertEqual(get_value(dup_job, "EMPLOYER_NAME"), "Pieper Farms, LLC")
        self.assertEqual(get_value(dup_job, "JOB_TITLE"), "Potato Farm Worker")
        self.assertEqual(get_value(dup_job, "housing_lat"), 48.866801)
        self.assertEqual(get_value(dup_job, "Source"), "DOL")
        self.assertEqual(get_value(dup_job, "WORKSITE_CITY"), "Williams")
        # testing columns from scraper
        self.assertEqual(get_value(dup_job, "Number of Pages"), 15)
        # making sure it worked on orher duplicate job
        self.assertEqual(get_value(all_accs1[all_accs1["CASE_NUMBER"] == "H-300-20119-524313"], "WORKSITE_CITY"), "Riceville")

accs_old2 = accurate_old_jobs.copy()
len_accs_old2 = len(accs_old2)
accs_old2.at[len_accs_old2 - 2, "fixed"] = True
accs_old2.at[len_accs_old2 - 2, "housing_fixed_by"] = "address"
accs_new2 = accurate_new_jobs.copy()
len_accs_new2 = len(accs_new2)
accs_new2.at[len_accs_new2 - 1, "CASE_NUMBER"] = "H-300-20125-538913"
all_accs2, all_inaccs2 = merge_all_data(accs_new2, inaccurate_new_jobs.copy(), accs_old2, inaccurate_old_jobs.copy())
class TestCaseTwo(unittest.TestCase):
    def test_lengths(self):
        self.assertEqual(len(all_accs2), 18)
        self.assertEqual(len(all_inaccs2), 9)
    def test_no_dups(self):
        self.assertTrue(all(has_no_dups(all_accs2, all_inaccs2)))
    def test_used_right_data(self):
        dup_job = all_accs2[all_accs2["CASE_NUMBER"] == "H-300-20125-538913"]
        self.assertEqual(get_value(dup_job, "EMPLOYER_NAME"), "Bio Application LLC")
        self.assertEqual(get_value(dup_job, "JOB_TITLE"), "Farmworkers and Laborers, Crop")
        self.assertEqual(get_value(dup_job, "worksite_long"), -92.582184)
        self.assertEqual(get_value(dup_job, "Source"), "DOL")
        self.assertEqual(get_value(dup_job, "HOUSING_ADDRESS_LOCATION"), "3272 440th St")
        self.assertEqual(get_value(dup_job, "W to H Ratio"), "W=H")


accs_new3 = accurate_new_jobs.copy()
accs_new3.at[len(accs_new3) - 1, "CASE_NUMBER"] = "H-300-20108-494660"
all_accs3, all_inaccs3 = merge_all_data(accs_new3, inaccurate_new_jobs.copy(), accurate_old_jobs.copy(), inaccurate_old_jobs.copy())
class TestCaseThree(unittest.TestCase):
    def test_lengths(self):
        self.assertEqual(len(all_accs3), 19)
        self.assertEqual(len(all_inaccs3), 8)
    def test_no_dups(self):
        self.assertTrue(all(has_no_dups(all_accs3, all_inaccs3)))
    def test_used_right_data(self):
        dup_job = all_accs3[all_accs3["CASE_NUMBER"] == "H-300-20108-494660"]
        self.assertEqual(get_value(dup_job, "EMPLOYER_NAME"), "Bio Application LLC")
        self.assertEqual(get_value(dup_job, "JOB_TITLE"), "Farmworkers and Laborers, Crop")
        self.assertEqual(get_value(dup_job, "worksite_long"), -92.582184)
        self.assertEqual(get_value(dup_job, "Source"), "DOL")
        self.assertEqual(get_value(dup_job, "HOUSING_ADDRESS_LOCATION"), "3272 440th St")
        self.assertEqual(get_value(dup_job, "W to H Ratio"), "W=H")


accs_new4 = accurate_new_jobs.copy()
accs_new4.at[len(accs_new4) - 1, "CASE_NUMBER"] = "H-300-20108-494660"
inaccs_old4 = inaccurate_old_jobs.copy()
inaccs_old4.at[len(inaccs_old4) - 1, "fixed"] = True
all_accs4, all_inaccs4 = merge_all_data(accs_new4, inaccurate_new_jobs.copy(), accurate_old_jobs.copy(), inaccs_old4)
class TestCaseFour(unittest.TestCase):
    def test_lengths(self):
        self.assertEqual(len(all_accs4), 19)
        self.assertEqual(len(all_inaccs4), 8)
    def test_no_dups(self):
        self.assertTrue(all(has_no_dups(all_accs4, all_inaccs4)))
    def test_used_right_data(self):
        dup_job = all_accs4[all_accs4["CASE_NUMBER"] == "H-300-20108-494660"]
        self.assertEqual(get_value(dup_job, "EMPLOYER_NAME"), "Bio Application LLC")
        self.assertEqual(get_value(dup_job, "JOB_TITLE"), "Farmworkers and Laborers, Crop")
        self.assertEqual(get_value(dup_job, "worksite_long"), -92.582184)
        self.assertEqual(get_value(dup_job, "Source"), "DOL")
        self.assertEqual(get_value(dup_job, "HOUSING_ADDRESS_LOCATION"), "3272 440th St")
        self.assertEqual(get_value(dup_job, "W to H Ratio"), "W=H")

inaccs_new5 = inaccurate_new_jobs.copy()
inaccs_new5.at[len(inaccs_new5) - 3, "CASE_NUMBER"] = "H-300-20121-530549"
all_accs5, all_inaccs5 = merge_all_data(accurate_new_jobs.copy(), inaccs_new5, accurate_old_jobs.copy(), inaccurate_old_jobs.copy())
class TestCaseFive(unittest.TestCase):
    def test_lengths(self):
        self.assertEqual(len(all_accs5), 18)
        self.assertEqual(len(all_inaccs5), 9)
    def test_no_dups(self):
        self.assertTrue(all(has_no_dups(all_accs5, all_inaccs5)))
    def test_used_right_data(self):
        dup_job = all_inaccs5[all_inaccs5["CASE_NUMBER"] == "H-300-20121-530549"]
        self.assertEqual(get_value(dup_job, "EMPLOYER_NAME"), "Leach Farms, Inc. ")
        self.assertEqual(get_value(dup_job, "JOB_TITLE"), "Field Workers (Celery Harvest) ")
        self.assertEqual(get_value(dup_job, "worksite_long"), -89.027124)
        self.assertEqual(get_value(dup_job, "Source"), "DOL")
        self.assertEqual(get_value(dup_job, "HOUSING_CITY"), "Berlin")
        self.assertEqual(get_value(dup_job, "W to H Ratio"), "W=H")


inaccs_new6i = inaccurate_new_jobs.copy()
inaccs_new6i.at[len(inaccs_new6i) - 3, "CASE_NUMBER"] = "H-300-20121-530549"
accs_old6i = accurate_old_jobs.copy()
job_in_acc_pos = len(accs_old6i) - 3
accs_old6i.at[job_in_acc_pos, "fixed"] = True
accs_old6i.at[job_in_acc_pos, "housing_fixed_by"] = "address"
accs_old6i.at[job_in_acc_pos, "worksite_fixed_by"] = "address"
accs_old6i.at[job_in_acc_pos, "worksite accuracy"] = 0.8
all_accs6i, all_inaccs6i = merge_all_data(accurate_new_jobs.copy(), inaccs_new6i, accs_old6i, inaccurate_old_jobs.copy())
class TestCaseSixI(unittest.TestCase):
    def test_lengths(self):
        self.assertEqual(len(all_accs6i), 19)
        self.assertEqual(len(all_inaccs6i), 8)
    def test_no_dups(self):
        self.assertTrue(all(has_no_dups(all_accs6i, all_inaccs6i)))
    def test_used_right_data(self):
        dup_job = all_accs6i[all_accs6i["CASE_NUMBER"] == "H-300-20121-530549"]
        self.assertEqual(get_value(dup_job, "EMPLOYER_NAME"), "Leach Farms, Inc. ")
        self.assertEqual(get_value(dup_job, "JOB_TITLE"), "Field Workers (Celery Harvest) ")
        self.assertEqual(get_value(dup_job, "Source"), "DOL")
        self.assertEqual(get_value(dup_job, "W to H Ratio"), "W=H")
        self.assertEqual(get_value(dup_job, "HOUSING_CITY"), "Pequea")
        self.assertEqual(get_value(dup_job, "HOUSING_ADDRESS_LOCATION"), "719 Marticville RD")
        self.assertEqual(get_value(dup_job, "HOUSING_POSTAL_CODE"), 17565)
        self.assertEqual(get_value(dup_job, "HOUSING_STATE"), "Pennsylvania")
        self.assertEqual(get_value(dup_job, "WORKSITE_ADDRESS"), "569 Marticville RD")
        self.assertEqual(get_value(dup_job, "WORKSITE_CITY"), "Pequea")
        self.assertEqual(get_value(dup_job, "WORKSITE_STATE"), "PENNSYLVANIA")
        self.assertEqual(get_value(dup_job, "WORKSITE_POSTAL_CODE"), 17565)
        self.assertEqual(get_value(dup_job, "housing_fixed_by"), "address")
        self.assertEqual(get_value(dup_job, "worksite_fixed_by"), "address")
        self.assertEqual(get_value(dup_job, "fixed"), True)
        self.assertEqual(get_value(dup_job, "worksite accuracy type"), "range_interpolation")
        self.assertEqual(get_value(dup_job, "housing accuracy type"), "rooftop")
        self.assertEqual(get_value(dup_job, "housing accuracy"), 1)
        self.assertEqual(get_value(dup_job, "worksite accuracy"), 0.8)
        self.assertEqual(get_value(dup_job, "housing_long"), -76.305982)
        self.assertEqual(get_value(dup_job, "housing_lat"), 39.925263)
        self.assertEqual(get_value(dup_job, "worksite_long"), -76.306133)
        self.assertEqual(get_value(dup_job, "worksite_lat"), 39.935672)


inaccs_new6ii = inaccurate_new_jobs.copy()
inaccs_new6ii.at[len(inaccs_new6ii) - 3, "CASE_NUMBER"] = "H-300-20121-530549"
accs_old6ii = accurate_old_jobs.copy()
job_in_acc_pos = len(accs_old6ii) - 3
accs_old6ii.at[job_in_acc_pos, "fixed"] = True
accs_old6ii.at[job_in_acc_pos, "housing_fixed_by"] = "coordinates"
accs_old6ii.at[job_in_acc_pos, "worksite_fixed_by"] = "coordinates"
all_accs6ii, all_inaccs6ii = merge_all_data(accurate_new_jobs.copy(), inaccs_new6ii, accs_old6ii, inaccurate_old_jobs.copy())
class TestCaseSixII(unittest.TestCase):
    def test_lengths(self):
        self.assertEqual(len(all_accs6ii), 19)
        self.assertEqual(len(all_inaccs6ii), 8)
    def test_no_dups(self):
        self.assertTrue(all(has_no_dups(all_accs6ii, all_inaccs6ii)))
    def test_used_right_data(self):
        dup_job = all_accs6ii[all_accs6ii["CASE_NUMBER"] == "H-300-20121-530549"]
        self.assertEqual(get_value(dup_job, "EMPLOYER_NAME"), "Leach Farms, Inc. ")
        self.assertEqual(get_value(dup_job, "JOB_TITLE"), "Field Workers (Celery Harvest) ")
        self.assertEqual(get_value(dup_job, "Source"), "DOL")
        self.assertEqual(get_value(dup_job, "W to H Ratio"), "W=H")
        self.assertEqual(get_value(dup_job, "HOUSING_CITY"), "Berlin")
        self.assertEqual(get_value(dup_job, "HOUSING_ADDRESS_LOCATION"), "W1026 Buttercup Court")
        self.assertEqual(get_value(dup_job, "HOUSING_POSTAL_CODE"), 54923)
        self.assertEqual(get_value(dup_job, "HOUSING_STATE"), "WISCONSIN")
        self.assertEqual(get_value(dup_job, "WORKSITE_ADDRESS"), "Leach Farms-W1102 Buttercup Ct")
        self.assertEqual(get_value(dup_job, "WORKSITE_CITY"), "Berlin")
        self.assertEqual(get_value(dup_job, "WORKSITE_STATE"), "WISCONSIN")
        self.assertEqual(get_value(dup_job, "WORKSITE_POSTAL_CODE"), 54923)
        self.assertEqual(get_value(dup_job, "housing_fixed_by"), "coordinates")
        self.assertEqual(get_value(dup_job, "worksite_fixed_by"), "coordinates")
        self.assertEqual(get_value(dup_job, "fixed"), True)
        self.assertEqual(get_value(dup_job, "worksite accuracy type"), "range_interpolation")
        self.assertEqual(get_value(dup_job, "housing accuracy type"), "rooftop")
        self.assertEqual(get_value(dup_job, "housing accuracy"), 1)
        self.assertEqual(get_value(dup_job, "worksite accuracy"), 1)
        self.assertEqual(get_value(dup_job, "housing_long"), -76.305982)
        self.assertEqual(get_value(dup_job, "housing_lat"), 39.925263)
        self.assertEqual(get_value(dup_job, "worksite_long"), -76.306133)
        self.assertEqual(get_value(dup_job, "worksite_lat"), 39.935672)

# case where worksite_fixed_by is impossible/na and worksite needs fixing in new data (acc type is "place" in this case)
inaccs_new6iiiA = inaccurate_new_jobs.copy()
inaccs_new6iiiA.at[len(inaccs_new6iiiA) - 3, "CASE_NUMBER"] = "H-300-20121-530549"
accs_old6iiiA = accurate_old_jobs.copy()
job_in_acc_pos = len(accs_old6iiiA) - 3
accs_old6iiiA.at[job_in_acc_pos, "fixed"] = True
accs_old6iiiA.at[job_in_acc_pos, "housing_fixed_by"] = "coordinates"
all_accs6iiiA, all_inaccs6iiiA = merge_all_data(accurate_new_jobs.copy(), inaccs_new6iiiA, accs_old6iiiA, inaccurate_old_jobs.copy())
class TestCaseSixIIIa(unittest.TestCase):
    def test_lengths(self):
        self.assertEqual(len(all_accs6iiiA), 18)
        self.assertEqual(len(all_inaccs6iiiA), 9)
    def test_no_dups(self):
        self.assertTrue(all(has_no_dups(all_accs6iiiA, all_inaccs6iiiA)))
    def test_used_right_data(self):
        dup_job = all_inaccs6iiiA[all_inaccs6iiiA["CASE_NUMBER"] == "H-300-20121-530549"]
        self.assertEqual(get_value(dup_job, "EMPLOYER_NAME"), "Leach Farms, Inc. ")
        self.assertEqual(get_value(dup_job, "JOB_TITLE"), "Field Workers (Celery Harvest) ")
        self.assertEqual(get_value(dup_job, "Source"), "DOL")
        self.assertEqual(get_value(dup_job, "W to H Ratio"), "W=H")
        self.assertEqual(get_value(dup_job, "HOUSING_CITY"), "Berlin")
        self.assertEqual(get_value(dup_job, "HOUSING_ADDRESS_LOCATION"), "W1026 Buttercup Court")
        self.assertEqual(get_value(dup_job, "HOUSING_POSTAL_CODE"), 54923)
        self.assertEqual(get_value(dup_job, "HOUSING_STATE"), "WISCONSIN")
        self.assertEqual(get_value(dup_job, "WORKSITE_ADDRESS"), "Leach Farms-W1102 Buttercup Ct")
        self.assertEqual(get_value(dup_job, "WORKSITE_CITY"), "Berlin")
        self.assertEqual(get_value(dup_job, "WORKSITE_STATE"), "WISCONSIN")
        self.assertEqual(get_value(dup_job, "WORKSITE_POSTAL_CODE"), 54923)
        self.assertEqual(get_value(dup_job, "housing_fixed_by"), "coordinates")
        self.assertTrue(pd.isnull(get_value(dup_job, "worksite_fixed_by")))
        self.assertEqual(get_value(dup_job, "fixed"), False)
        self.assertEqual(get_value(dup_job, "worksite accuracy type"), "place")
        self.assertEqual(get_value(dup_job, "housing accuracy type"), "rooftop")
        self.assertEqual(get_value(dup_job, "housing accuracy"), 1)
        self.assertEqual(get_value(dup_job, "worksite accuracy"), 1)
        self.assertEqual(get_value(dup_job, "housing_long"), -76.305982)
        self.assertEqual(get_value(dup_job, "housing_lat"), 39.925263)
        self.assertEqual(get_value(dup_job, "worksite_long"), -89.027124)
        self.assertEqual(get_value(dup_job, "worksite_lat"), 43.915726)

#case where housing_fixed_by is impossible/na and housing doesn't need fixing in new data
inaccs_new6iiiB = inaccurate_new_jobs.copy()
inaccs_new6iiiB.at[len(inaccs_new6iiiB) - 3, "CASE_NUMBER"] = "H-300-20121-530549"
accs_old6iiiB = accurate_old_jobs.copy()
job_in_acc_pos = len(accs_old6iiiB) - 3
accs_old6iiiB.at[job_in_acc_pos, "fixed"] = True
accs_old6iiiB.at[job_in_acc_pos, "worksite_fixed_by"] = "coordinates"
all_accs6iiiB, all_inaccs6iiiB = merge_all_data(accurate_new_jobs.copy(), inaccs_new6iiiB, accs_old6iiiB, inaccurate_old_jobs.copy())
class TestCaseSixIIIb(unittest.TestCase):
    def test_lengths(self):
        self.assertEqual(len(all_accs6iiiB), 19)
        self.assertEqual(len(all_inaccs6iiiB), 8)
    def test_no_dups(self):
        self.assertTrue(all(has_no_dups(all_accs6iiiB, all_inaccs6iiiB)))
    def test_used_right_data(self):
        dup_job = all_accs6iiiB[all_accs6iiiB["CASE_NUMBER"] == "H-300-20121-530549"]
        self.assertEqual(get_value(dup_job, "EMPLOYER_NAME"), "Leach Farms, Inc. ")
        self.assertEqual(get_value(dup_job, "JOB_TITLE"), "Field Workers (Celery Harvest) ")
        self.assertEqual(get_value(dup_job, "Source"), "DOL")
        self.assertEqual(get_value(dup_job, "W to H Ratio"), "W=H")
        self.assertEqual(get_value(dup_job, "HOUSING_CITY"), "Berlin")
        self.assertEqual(get_value(dup_job, "HOUSING_ADDRESS_LOCATION"), "W1026 Buttercup Court")
        self.assertEqual(get_value(dup_job, "HOUSING_POSTAL_CODE"), 54923)
        self.assertEqual(get_value(dup_job, "HOUSING_STATE"), "WISCONSIN")
        self.assertEqual(get_value(dup_job, "WORKSITE_ADDRESS"), "Leach Farms-W1102 Buttercup Ct")
        self.assertEqual(get_value(dup_job, "WORKSITE_CITY"), "Berlin")
        self.assertEqual(get_value(dup_job, "WORKSITE_STATE"), "WISCONSIN")
        self.assertEqual(get_value(dup_job, "WORKSITE_POSTAL_CODE"), 54923)
        self.assertTrue(pd.isnull(get_value(dup_job, "housing_fixed_by")))
        self.assertEqual(get_value(dup_job, "worksite_fixed_by"), "coordinates")
        self.assertEqual(get_value(dup_job, "fixed"), True)
        self.assertEqual(get_value(dup_job, "worksite accuracy type"), "range_interpolation")
        self.assertEqual(get_value(dup_job, "housing accuracy type"), "rooftop")
        self.assertEqual(get_value(dup_job, "housing accuracy"), 1)
        self.assertEqual(get_value(dup_job, "worksite accuracy"), 1)
        self.assertEqual(get_value(dup_job, "housing_long"), -88.932913)
        self.assertEqual(get_value(dup_job, "housing_lat"), 44.080442)
        self.assertEqual(get_value(dup_job, "worksite_long"), -76.306133)
        self.assertEqual(get_value(dup_job, "worksite_lat"), 39.935672)

inaccs_new7 = inaccurate_new_jobs.copy()
inaccs_new7.at[len(inaccs_new7) - 1, "CASE_NUMBER"] = "H-300-20114-511870"
all_accs7, all_inaccs7 = merge_all_data(accurate_new_jobs.copy(), inaccs_new7, accurate_old_jobs.copy(), inaccurate_old_jobs.copy())
class TestCaseSeven(unittest.TestCase):
    def test_lengths(self):
        self.assertEqual(len(all_accs7), 19)
        self.assertEqual(len(all_inaccs7), 8)
    def test_no_dups(self):
        self.assertTrue(all(has_no_dups(all_accs7, all_inaccs7)))
    def test_used_right_data(self):
        dup_job = all_inaccs7[all_inaccs7["CASE_NUMBER"] == "H-300-20114-511870"]
        self.assertEqual(get_value(dup_job, "EMPLOYER_NAME"), "Alstede Farms, LLC")
        self.assertEqual(get_value(dup_job, "JOB_TITLE"), "Farm Worker")
        self.assertEqual(get_value(dup_job, "worksite_long"), -74.683861)
        self.assertEqual(get_value(dup_job, "Source"), "DOL")
        self.assertEqual(get_value(dup_job, "HOUSING_CITY"), "Chester")
        self.assertEqual(get_value(dup_job, "W to H Ratio"), "W=H")

inaccs_new8 = inaccurate_new_jobs.copy()
inaccs_new8.at[len(inaccs_new8) - 1, "CASE_NUMBER"] = "H-300-20114-511870"
inaccs_new8.at[len(inaccs_new8) - 1, "housing accuracy type"] = "rooftop"
inaccs_new8.at[len(inaccs_new8) - 1, "housing accuracy"] = 1.0
inaccs_old8 = inaccurate_old_jobs.copy()
inaccs_old8.at[len(inaccs_old8) - 2, "fixed"] = True
inaccs_old8.at[len(inaccs_old8) - 2, "worksite_fixed_by"] = "address"
inaccs_old8.at[len(inaccs_old8) - 2, "WORKSITE_POSTAL_CODE"] = 11111
inaccs_old8.at[len(inaccs_old8) - 2, "worksite accuracy type"] = "roof"
inaccs_old8.at[len(inaccs_old8) - 2, "worksite accuracy"] = 0.999
inaccs_old8.at[len(inaccs_old8) - 2, "worksite_long"] = 1000
all_accs8, all_inaccs8 = merge_all_data(accurate_new_jobs.copy(), inaccs_new8, accurate_old_jobs.copy(), inaccs_old8)
class TestCaseEight(unittest.TestCase):
    def test_lengths(self):
        self.assertEqual(len(all_accs8), 19)
        self.assertEqual(len(all_inaccs8), 8)
    def test_no_dups(self):
        self.assertTrue(all(has_no_dups(all_accs8, all_inaccs8)))
    def test_used_right_data(self):
        dup_job = all_inaccs8[all_inaccs8["CASE_NUMBER"] == "H-300-20114-511870"]
        self.assertEqual(get_value(dup_job, "EMPLOYER_NAME"), "Alstede Farms, LLC")
        self.assertEqual(get_value(dup_job, "JOB_TITLE"), "Farm Worker")
        self.assertEqual(get_value(dup_job, "Source"), "DOL")
        self.assertEqual(get_value(dup_job, "W to H Ratio"), "W=H")
        self.assertEqual(get_value(dup_job, "HOUSING_CITY"), "Chester")
        self.assertEqual(get_value(dup_job, "HOUSING_ADDRESS_LOCATION"), "1 Alstede Farms Lane")
        self.assertEqual(get_value(dup_job, "HOUSING_POSTAL_CODE"), 7930)
        self.assertEqual(get_value(dup_job, "HOUSING_STATE"), "NEW JERSEY")
        self.assertEqual(get_value(dup_job, "WORKSITE_ADDRESS"), "407B Road St-Just, T9R18, mile 11")
        self.assertEqual(get_value(dup_job, "WORKSITE_CITY"), "Somerset County")
        self.assertEqual(get_value(dup_job, "WORKSITE_STATE"), "MAINE")
        self.assertEqual(get_value(dup_job, "WORKSITE_POSTAL_CODE"), 11111)
        self.assertTrue(pd.isnull(get_value(dup_job, "housing_fixed_by")))
        self.assertEqual(get_value(dup_job, "worksite_fixed_by"), "address")
        self.assertEqual(get_value(dup_job, "worksite accuracy type"), "roof")
        self.assertEqual(get_value(dup_job, "housing accuracy type"), "rooftop")
        self.assertEqual(get_value(dup_job, "housing accuracy"), 1.0)
        self.assertEqual(get_value(dup_job, "worksite accuracy"), 0.999)
        self.assertEqual(get_value(dup_job, "housing_long"), -74.683861)
        self.assertEqual(get_value(dup_job, "housing_lat"), 40.782111)
        self.assertEqual(get_value(dup_job, "worksite_long"), 1000)
        self.assertTrue(pd.isnull(get_value(dup_job, "worksite_lat")))
        self.assertEqual(get_value(dup_job, "fixed"), True)

inaccs_old_w_dolH = inaccurate_old_jobs.copy()
last_row = inaccs_old_w_dolH[inaccs_old_w_dolH["CASE_NUMBER"] == "H-300-20108-494660"]
inaccs_old_w_dolH = inaccs_old_w_dolH.append(last_row)
inaccs_old_w_dolH = inaccs_old_w_dolH.append(last_row)
inaccs_old_w_dolH = inaccs_old_w_dolH.reset_index()
len_inaccs_old = len(inaccs_old_w_dolH)
inaccs_old_w_dolH.at[len_inaccs_old - 1, "table"] = "dol_h"
inaccs_old_w_dolH.at[len_inaccs_old - 2, "table"] = "dol_h"
inaccs_new_dolH_test = inaccurate_new_jobs.copy()
inaccs_new_dolH_test.at[len(inaccs_new_dolH_test) - 1, "CASE_NUMBER"] = "H-300-20108-494660"
all_accs_dolH_test, all_inaccs_dolH_test = merge_all_data(accurate_new_jobs.copy(), inaccs_new_dolH_test, accurate_old_jobs.copy(), inaccs_old_w_dolH)

class TestKeepsDOL_Hs(unittest.TestCase):
    def test_lengths(self):
        self.assertEqual(len(all_accs_dolH_test), 19)
        self.assertEqual(len(all_inaccs_dolH_test), 10)
    def has_duplicates(self):
        accs_no_dups, inaccs_no_dups, no_dups_anywhere = has_no_dups(all_accs_dolH_test, all_inaccs_dolH_test)
        self.assertTrue(accs_no_dups)
        self.assertEqual(inaccs_no_dups, False)
        self.assertEqual(no_dups_anywhere, False)

class TestAnEntireCycle(unittest.TestCase):
    pass


unittest.main(verbosity=2)
