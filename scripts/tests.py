"""Tests for various functions within the app. Uses unittest library."""

import helpers

if helpers.force_cloud:
    print("Refusing to run tests. Never run tests on the actual database!!! You are seeing this alert because the force_cloud variable is set to True in helpers.py. Set it to False to run tests on the local database.")
    exit()

import unittest
from populate_database import geocode_manage_split
from add_housing import geocode_manage_split_housing
from merge_dol import geocode_manage_split_merge
from implement_fixes import implement_fixes
from helpers import merge_all_data, get_value, myprint, make_query, geocode_table
from colorama import Fore, Style
import os
import pandas as pd
bad_accuracy_types = helpers.bad_accuracy_types
engine = helpers.get_database_engine()

def set_test_database_state(accurates, inaccurates):
    make_query("DELETE FROM job_central")
    make_query("DELETE FROM low_accuracies")
    accurates.to_sql("job_central", engine, if_exists='append', index=False, dtype=helpers.column_types)
    inaccurates.to_sql("low_accuracies", engine, if_exists='append', index=False, dtype=helpers.column_types)
    make_query("REFRESH MATERIALIZED VIEW previously_geocoded")

def merge_all_and_get_new_state(accurate_new_jobs, inaccurate_new_jobs):
    merge_all_data(accurate_new_jobs, inaccurate_new_jobs)
    accurates = pd.read_sql("job_central", con=engine)
    inaccurates = pd.read_sql("low_accuracies", con=engine)
    return accurates, inaccurates

def has_no_dups(accurates, inaccurates):
    accurate_case_numbers = accurates["CASE_NUMBER"].tolist()
    accs_no_dups = (len(accurate_case_numbers) == len(set(accurate_case_numbers)))
    inaccurate_case_numbers = inaccurates["CASE_NUMBER"].tolist()
    inaccs_no_dups = (len(inaccurate_case_numbers) == len(set(inaccurate_case_numbers)))
    all_case_numbers = accurate_case_numbers + inaccurate_case_numbers
    no_dups_anywhere = len(all_case_numbers) == len(set(all_case_numbers))
    return accs_no_dups, inaccs_no_dups, no_dups_anywhere

def assert_accuracies_and_inaccuracies(accurates, inaccurates):
    worksites_accurate = ((accurates["worksite accuracy"] >= 0.7) & (~accurates["worksite accuracy type"].isin(bad_accuracy_types))).all()
    accurates_h2a = accurates[accurates["Visa type"] == "H-2A"]
    housings_accurate = ((accurates_h2a["housing accuracy"] >= 0.7) & (~accurates_h2a["housing accuracy type"].isin(bad_accuracy_types))).all()
    inaccurates_h2a = inaccurates[inaccurates["Visa type"] == "H-2A"]
    inaccurate_conditions_h2a = ((inaccurates_h2a["housing accuracy"].isnull()) | (inaccurates_h2a["worksite accuracy"].isnull()) | (inaccurates_h2a["housing accuracy"] < 0.7) | (inaccurates_h2a["worksite accuracy"] < 0.7) | (inaccurates_h2a["housing accuracy type"].isin(bad_accuracy_types)) | (inaccurates_h2a["worksite accuracy type"].isin(bad_accuracy_types)))
    h2a_inaccurates_inaccurate = inaccurate_conditions_h2a.all()
    inaccurates_h2b = inaccurates[inaccurates["Visa type"] == "H-2B"]
    if len(inaccurates_h2b) != 0:
        inaccurate_conditions_h2b = ((inaccurates["worksite accuracy"].isnull()) | (inaccurates_h2b["worksite accuracy"] < 0.7) | (inaccurates_h2b["worksite accuracy type"].isin(bad_accuracy_types)))
        h2b_inaccurates_inaccurate = inaccurate_conditions_h2b.all()
    else:
        h2b_inaccurates_inaccurate = True
    return worksites_accurate, housings_accurate, h2a_inaccurates_inaccurate, h2b_inaccurates_inaccurate

type_conversions = {'fixed': bool, 'housing_fixed_by': str, 'worksite_fixed_by': str, "HOUSING_POSTAL_CODE": str, "WORKSITE_POSTAL_CODE": str}
accurate_new_jobs = pd.read_excel(os.path.join(os.getcwd(), '..',  "excel_files/accurate_dol_geocoded.xlsx"), converters=type_conversions)
inaccurate_new_jobs = pd.read_excel(os.path.join(os.getcwd(), '..',  "excel_files/inaccurate_dol_geocoded.xlsx"),  converters=type_conversions)
accurate_old_jobs = pd.read_excel(os.path.join(os.getcwd(), '..',  "excel_files/accurates_geocoded.xlsx"), converters=type_conversions)
inaccurate_old_jobs = pd.read_excel(os.path.join(os.getcwd(), '..',  "excel_files/inaccurates_geocoded.xlsx"),  converters=type_conversions)

myprint("Start of add housing test", is_red="red")
housing = pd.read_excel(os.path.join(os.getcwd(), '..', 'excel_files/housing_addendum.xlsx'))
accurate_housing, inaccurate_housing = geocode_manage_split_housing(housing, 2020, 3)
class TestAddHousing(unittest.TestCase):
    def test_length_and_table_column(self):
        self.assertEqual(len(accurate_housing), 9)
        self.assertEqual(len(inaccurate_housing), 1)
        self.assertTrue((accurate_housing["table"] == "dol_h").all() and (inaccurate_housing["table"] == "dol_h").all())
    def test_accuracies(self):
        accurates_are_accurate = (((accurate_housing["housing accuracy"] >= 0.7) & (~(accurate_housing["housing accuracy type"].isin(bad_accuracy_types)))) | (~accurate_housing["HOUSING_STATE"].str.lower().isin(helpers.our_states))).all()
        self.assertTrue(accurates_are_accurate)
        inaccurates_are_inaccurate = ((inaccurate_housing["housing accuracy"].isnull()) | (inaccurate_housing["housing accuracy"] < 0.7) | (inaccurate_housing["housing accuracy type"].isin(bad_accuracy_types))).all()
        self.assertTrue(inaccurates_are_inaccurate)
    def test_fy_column(self):
        self.assertTrue(all(accurate_housing["fy"] == "2020Q3"))
        self.assertTrue(all(inaccurate_housing["fy"] == "2020Q3"))

myprint("Start of previously geocoded test")
accurates = pd.read_excel(os.path.join(os.getcwd(), '..',  "excel_files/accurates_geocoded_prev_geocoded_test.xlsx"), converters=type_conversions)
inaccurates = pd.read_excel(os.path.join(os.getcwd(), '..',  "excel_files/inaccurates_geocoded_prev_geocoded_test.xlsx"),  converters=type_conversions)
set_test_database_state(accurates, inaccurates)
news = pd.read_excel(os.path.join(os.getcwd(), '..',  "excel_files/new_jobs_prev_fixed_test.xlsx"),  converters=type_conversions)
geocoded = geocode_table(news, "housing", check_previously_geocoded=True).reset_index()
class TestPreviouslyGeocoded(unittest.TestCase):
    def test_values(self):
        self.assertEqual(geocoded.at[1, "housing_lat"], 22)
        self.assertEqual(geocoded.at[1, "housing_long"], 90)
        self.assertEqual(geocoded.at[1, "housing accuracy"], 1)
        self.assertEqual(geocoded.at[1, "housing accuracy type"], "rooftop")
        self.assertEqual(geocoded.at[2, "housing_lat"], 123)
        self.assertEqual(geocoded.at[2, "housing_long"], 133)
        self.assertEqual(geocoded.at[2, "housing accuracy"], 100)
        self.assertEqual(geocoded.at[2, "housing accuracy type"], "aaa")

# TESTING MERGE ALL DATA FUNCTION - see project documentation to see what is meant by case_1 through case_8
myprint("Start of test case 0", is_red="red")
set_test_database_state(accurate_old_jobs, inaccurate_old_jobs)
all_accurate_jobs, all_inaccurate_jobs = merge_all_and_get_new_state(accurate_new_jobs, inaccurate_new_jobs)
class TestCaseZero(unittest.TestCase):
    def test_initital_case(self):
        self.assertEqual(len(all_accurate_jobs), 19)
        self.assertEqual(len(all_inaccurate_jobs), 9)
        self.assertTrue(all(has_no_dups(all_accurate_jobs, all_inaccurate_jobs)))

acc_new1 = accurate_new_jobs.copy()
acc_new1_len = len(acc_new1)
acc_new1.at[acc_new1_len - 1, "CASE_NUMBER"] = "H-300-20119-524313"
acc_new1.at[acc_new1_len - 2, "CASE_NUMBER"] = "H-300-20125-538913"
myprint("Start of test case 1", is_red="red")
set_test_database_state(accurate_old_jobs, inaccurate_old_jobs)
all_accs1, all_inaccs1 = merge_all_and_get_new_state(acc_new1, inaccurate_new_jobs)
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
        self.assertEqual(get_value(dup_job, "WORKSITE_POSTAL_CODE"), "42743")

        # testing columns from scraper
        self.assertEqual(get_value(dup_job, "Number of Pages"), 15)
        # making sure it worked on other duplicate job
        self.assertEqual(get_value(all_accs1[all_accs1["CASE_NUMBER"] == "H-300-20119-524313"], "WORKSITE_CITY"), "Riceville")

accs_old2 = accurate_old_jobs.copy()
len_accs_old2 = len(accs_old2)
accs_old2.at[len_accs_old2 - 2, "fixed"] = True
accs_old2.at[len_accs_old2 - 2, "housing_fixed_by"] = "address"
accs_new2 = accurate_new_jobs.copy()
len_accs_new2 = len(accs_new2)
accs_new2.at[len_accs_new2 - 1, "CASE_NUMBER"] = "H-300-20125-538913"
myprint("Start of test case 2", is_red="red")
set_test_database_state(accs_old2, inaccurate_old_jobs)
all_accs2, all_inaccs2 = merge_all_and_get_new_state(accs_new2, inaccurate_new_jobs)
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
myprint("Start of test case 3", is_red="red")
set_test_database_state(accurate_old_jobs, inaccurate_old_jobs)
all_accs3, all_inaccs3 = merge_all_and_get_new_state(accs_new3, inaccurate_new_jobs)
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
myprint("Start of test case 4", is_red="red")
set_test_database_state(accurate_old_jobs, inaccs_old4)
all_accs4, all_inaccs4 = merge_all_and_get_new_state(accs_new4, inaccurate_new_jobs)
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
myprint("Start of test case 5", is_red="red")
set_test_database_state(accurate_old_jobs, inaccurate_old_jobs)
all_accs5, all_inaccs5 = merge_all_and_get_new_state(accurate_new_jobs, inaccs_new5)
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
accs_old6i.at[job_in_acc_pos, "worksite accuracy"] = 0.7
myprint("Start of test case 6I", is_red="red")
set_test_database_state(accs_old6i, inaccurate_old_jobs)
all_accs6i, all_inaccs6i = merge_all_and_get_new_state(accurate_new_jobs, inaccs_new6i)
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
        self.assertEqual(get_value(dup_job, "HOUSING_POSTAL_CODE"), '17565')
        self.assertEqual(get_value(dup_job, "HOUSING_STATE"), "Pennsylvania")
        self.assertEqual(get_value(dup_job, "WORKSITE_ADDRESS"), "569 Marticville RD")
        self.assertEqual(get_value(dup_job, "WORKSITE_CITY"), "Pequea")
        self.assertEqual(get_value(dup_job, "WORKSITE_STATE"), "PENNSYLVANIA")
        self.assertEqual(get_value(dup_job, "WORKSITE_POSTAL_CODE"), '17565')
        self.assertEqual(get_value(dup_job, "housing_fixed_by"), "address")
        self.assertEqual(get_value(dup_job, "worksite_fixed_by"), "address")
        self.assertEqual(get_value(dup_job, "fixed"), True)
        self.assertEqual(get_value(dup_job, "worksite accuracy type"), "range_interpolation")
        self.assertEqual(get_value(dup_job, "housing accuracy type"), "rooftop")
        self.assertEqual(get_value(dup_job, "housing accuracy"), 1)
        self.assertEqual(get_value(dup_job, "worksite accuracy"), 0.7)
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
myprint("Start of test case 6II", is_red="red")
set_test_database_state(accs_old6ii, inaccurate_old_jobs)
all_accs6ii, all_inaccs6ii = merge_all_and_get_new_state(accurate_new_jobs, inaccs_new6ii)
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
        self.assertEqual(get_value(dup_job, "HOUSING_POSTAL_CODE"), '54923')
        self.assertEqual(get_value(dup_job, "HOUSING_STATE"), "WISCONSIN")
        self.assertEqual(get_value(dup_job, "WORKSITE_ADDRESS"), "Leach Farms-W1102 Buttercup Ct")
        self.assertEqual(get_value(dup_job, "WORKSITE_CITY"), "Berlin")
        self.assertEqual(get_value(dup_job, "WORKSITE_STATE"), "WISCONSIN")
        self.assertEqual(get_value(dup_job, "WORKSITE_POSTAL_CODE"), '54923')
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
myprint("Start of test case 6iiiA", is_red="red")
set_test_database_state(accs_old6iiiA, inaccurate_old_jobs)
all_accs6iiiA, all_inaccs6iiiA = merge_all_and_get_new_state(accurate_new_jobs, inaccs_new6iiiA)
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
        self.assertEqual(get_value(dup_job, "HOUSING_POSTAL_CODE"), '54923')
        self.assertEqual(get_value(dup_job, "HOUSING_STATE"), "WISCONSIN")
        self.assertEqual(get_value(dup_job, "WORKSITE_ADDRESS"), "Leach Farms-W1102 Buttercup Ct")
        self.assertEqual(get_value(dup_job, "WORKSITE_CITY"), "Berlin")
        self.assertEqual(get_value(dup_job, "WORKSITE_STATE"), "WISCONSIN")
        self.assertEqual(get_value(dup_job, "WORKSITE_POSTAL_CODE"), '54923')
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


# case where housing_fixed_by is impossible/na and housing doesn't need fixing in new data
inaccs_new6iiiB = inaccurate_new_jobs.copy()
inaccs_new6iiiB.at[len(inaccs_new6iiiB) - 3, "CASE_NUMBER"] = "H-300-20121-530549"
accs_old6iiiB = accurate_old_jobs.copy()
job_in_acc_pos = len(accs_old6iiiB) - 3
accs_old6iiiB.at[job_in_acc_pos, "fixed"] = True
accs_old6iiiB.at[job_in_acc_pos, "worksite_fixed_by"] = "coordinates"
myprint("Start of test case 6iiiB", is_red="red")
set_test_database_state(accs_old6iiiB, inaccurate_old_jobs)
all_accs6iiiB, all_inaccs6iiiB = merge_all_and_get_new_state(accurate_new_jobs, inaccs_new6iiiB)
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
        self.assertEqual(get_value(dup_job, "HOUSING_POSTAL_CODE"), '54923')
        self.assertEqual(get_value(dup_job, "HOUSING_STATE"), "WISCONSIN")
        self.assertEqual(get_value(dup_job, "WORKSITE_ADDRESS"), "Leach Farms-W1102 Buttercup Ct")
        self.assertEqual(get_value(dup_job, "WORKSITE_CITY"), "Berlin")
        self.assertEqual(get_value(dup_job, "WORKSITE_STATE"), "WISCONSIN")
        self.assertEqual(get_value(dup_job, "WORKSITE_POSTAL_CODE"), '54923')
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
myprint("Start of test case 7", is_red="red")
set_test_database_state(accurate_old_jobs, inaccurate_old_jobs)
all_accs7, all_inaccs7 = merge_all_and_get_new_state(accurate_new_jobs, inaccs_new7)
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
len_inaccs_new8 = len(inaccs_new8)
inaccs_new8.at[len_inaccs_new8 - 1, "CASE_NUMBER"] = "H-300-20114-511870"
inaccs_new8.at[len_inaccs_new8 - 1, "housing accuracy type"] = "rooftop"
inaccs_new8.at[len_inaccs_new8 - 1, "housing accuracy"] = 1.0
inaccs_old8 = inaccurate_old_jobs.copy()
len_inaccs_old8 = len(inaccs_old8)
inaccs_old8.at[len_inaccs_old8 - 2, "fixed"] = True
inaccs_old8.at[len_inaccs_old8 - 2, "worksite_fixed_by"] = "address"
inaccs_old8.at[len_inaccs_old8 - 2, "WORKSITE_POSTAL_CODE"] = 11111
inaccs_old8.at[len_inaccs_old8 - 2, "worksite accuracy type"] = "roof"
inaccs_old8.at[len_inaccs_old8 - 2, "worksite accuracy"] = 0.999
inaccs_old8.at[len_inaccs_old8 - 2, "worksite_long"] = 1000
myprint("Start of test case 8", is_red="red")
set_test_database_state(accurate_old_jobs, inaccs_old8)
all_accs8, all_inaccs8 = merge_all_and_get_new_state(accurate_new_jobs, inaccs_new8)
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
        self.assertEqual(get_value(dup_job, "HOUSING_POSTAL_CODE"), '07930')
        self.assertEqual(get_value(dup_job, "HOUSING_STATE"), "NEW JERSEY")
        self.assertEqual(get_value(dup_job, "WORKSITE_ADDRESS"), "407B Road St-Just, T9R18, mile 11")
        self.assertEqual(get_value(dup_job, "WORKSITE_CITY"), "Somerset County")
        self.assertEqual(get_value(dup_job, "WORKSITE_STATE"), "MAINE")
        self.assertEqual(get_value(dup_job, "WORKSITE_POSTAL_CODE"), '11111')
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
inaccs_old_w_dolH = inaccs_old_w_dolH.reset_index().drop(columns=["index"])
len_inaccs_old = len(inaccs_old_w_dolH)
inaccs_old_w_dolH.at[len_inaccs_old - 1, "table"] = "dol_h"
inaccs_old_w_dolH.at[len_inaccs_old - 2, "table"] = "dol_h"
inaccs_new_dolH_test = inaccurate_new_jobs.copy()
inaccs_new_dolH_test.at[len(inaccs_new_dolH_test) - 1, "CASE_NUMBER"] = "H-300-20108-494660"
set_test_database_state(accurate_old_jobs, inaccs_old_w_dolH)
all_accs_dolH_test, all_inaccs_dolH_test = merge_all_and_get_new_state(accurate_new_jobs, inaccs_new_dolH_test)
class TestKeepsDOL_Hs(unittest.TestCase):
    def test_lengths(self):
        self.assertEqual(len(all_accs_dolH_test), 19)
        self.assertEqual(len(all_inaccs_dolH_test), 10)
    def test_has_duplicates(self):
        accs_no_dups, inaccs_no_dups, no_dups_anywhere = has_no_dups(all_accs_dolH_test, all_inaccs_dolH_test)
        self.assertTrue(accs_no_dups)
        self.assertEqual(inaccs_no_dups, False)
        self.assertEqual(no_dups_anywhere, False)


# Hamilton college: 198 College Hill Rd, Clinton, NY 13323  -  43.051469, -75.402153, 1, rooftop
# Vassar college: 124 Raymond Ave, Poughkeepsie NY 12604  -  41.686518, -73.897729, 1, rooftop
inaccurates_if_test = pd.read_excel(os.path.join(os.getcwd(), '..',  "excel_files/implement_fixes_tester.xlsx"))
myprint("Start of implement fixes test", is_red="red")
successes, housing_successes, failures = implement_fixes(inaccurates_if_test, fix_worksites=True)
class TestImplementFixes(unittest.TestCase):
    def test_both_fixed_by_address(self):
        job = successes[successes["CASE_NUMBER"] == "H-300-20227-769297"]
        self.assertEqual(get_value(job, "HOUSING_ADDRESS_LOCATION"), "198 College Hill Rd")
        self.assertEqual(get_value(job, "HOUSING_CITY"), "Clinton")
        self.assertEqual(get_value(job, "HOUSING_POSTAL_CODE"), 13323)
        self.assertEqual(get_value(job, "HOUSING_STATE"), "NY")
        self.assertEqual(get_value(job, "housing accuracy"), 1)
        self.assertEqual(get_value(job, "housing accuracy type"), "rooftop")
        self.assertEqual(get_value(job, "housing_lat"), 43.051469)
        self.assertEqual(get_value(job, "housing_long"), -75.402153)
        self.assertEqual(get_value(job, "housing_fixed_by"), "address")
        self.assertEqual(get_value(job, "WORKSITE_ADDRESS"), "124 Raymond Ave")
        self.assertEqual(get_value(job, "WORKSITE_CITY"), "Poughkeepsie")
        self.assertEqual(get_value(job, "worksite accuracy"), 1)
        self.assertEqual(get_value(job, "worksite accuracy type"), "rooftop")
        self.assertEqual(get_value(job, "worksite_lat"), 41.686518)
        self.assertEqual(get_value(job, "worksite_long"), -73.897729)
        self.assertEqual(get_value(job, "worksite_fixed_by"), "address")
        self.assertTrue(get_value(job, "fixed"))

    def test_both_fixed_by_coords(self):
        job = successes[successes["CASE_NUMBER"] == "H-300-20154-620944"]
        self.assertEqual(get_value(job, "HOUSING_CITY"), "Hammett")
        self.assertEqual(get_value(job, "HOUSING_POSTAL_CODE"), 83627)
        self.assertEqual(get_value(job, "housing accuracy"), 1)
        self.assertEqual(get_value(job, "housing accuracy type"), "rooftop")
        self.assertEqual(get_value(job, "housing_lat"), 22)
        self.assertEqual(get_value(job, "housing_long"), 21)
        self.assertEqual(get_value(job, "housing_fixed_by"), "coordinates")
        self.assertEqual(get_value(job, "WORKSITE_ADDRESS"), "8895 West Spangler Road (Including fields within a 15 mile radius)")
        self.assertEqual(get_value(job, "worksite accuracy"), 1)
        self.assertEqual(get_value(job, "worksite accuracy type"), "rooftop")
        self.assertEqual(get_value(job, "worksite_lat"), 33)
        self.assertEqual(get_value(job, "worksite_long"), 24)
        self.assertEqual(get_value(job, "worksite_fixed_by"), "coordinates")
        self.assertTrue(get_value(job, "fixed"))

    def test_one_coords_one_add(self):
        job = successes[successes["CASE_NUMBER"] == "H-300-20163-644868"]
        self.assertEqual(get_value(job, "HOUSING_CITY"), "Clinton")
        self.assertEqual(get_value(job, "HOUSING_POSTAL_CODE"), 13323)
        self.assertEqual(get_value(job, "housing accuracy"), 1)
        self.assertEqual(get_value(job, "housing accuracy type"), "rooftop")
        self.assertEqual(get_value(job, "housing_lat"), 43.051469)
        self.assertEqual(get_value(job, "housing_long"), -75.402153)
        self.assertEqual(get_value(job, "housing_fixed_by"), "address")
        self.assertEqual(get_value(job, "WORKSITE_ADDRESS"), "14095 N. Peyton Highway")
        self.assertEqual(get_value(job, "worksite accuracy"), 1)
        self.assertEqual(get_value(job, "worksite accuracy type"), "rooftop")
        self.assertEqual(get_value(job, "worksite_lat"), 100)
        self.assertEqual(get_value(job, "worksite_long"), 99)
        self.assertEqual(get_value(job, "worksite_fixed_by"), "coordinates")
        self.assertTrue(get_value(job, "fixed"))

    def test_one_impossible(self):
        job = successes[successes["CASE_NUMBER"] == "H-300-20163-644097"]
        self.assertEqual(get_value(job, "HOUSING_CITY"), "Fife")
        self.assertEqual(get_value(job, "housing accuracy"), 0.67)
        self.assertEqual(get_value(job, "housing accuracy type"), "place")
        self.assertEqual(get_value(job, "housing_long"), -122.359432)
        self.assertEqual(get_value(job, "housing_fixed_by"), "impossible")
        self.assertEqual(get_value(job, "WORKSITE_ADDRESS"), "3601 54th Ave E")
        self.assertEqual(get_value(job, "worksite accuracy"), 0.9)
        self.assertEqual(get_value(job, "worksite accuracy type"), "range_interpolation")
        self.assertEqual(get_value(job, "worksite_long"), -122.3572)
        self.assertTrue(pd.isnull(get_value(job, "worksite_fixed_by")))
        self.assertTrue(get_value(job, "fixed"))

    def test_both_impossible(self):
        job = successes[successes["CASE_NUMBER"] == "H-300-20108-494660"]
        self.assertEqual(get_value(job, "HOUSING_CITY"), "Hollister")
        self.assertEqual(get_value(job, "housing accuracy"), 1)
        self.assertEqual(get_value(job, "housing_long"), -121.402368)
        self.assertEqual(get_value(job, "housing_fixed_by"), "impossible")
        self.assertEqual(get_value(job, "WORKSITE_ADDRESS"), "Ranch 12: 95 Mc Fadden Road")
        self.assertEqual(get_value(job, "worksite accuracy type"), "place")
        self.assertEqual(get_value(job, "worksite_long"), -121.599444)
        self.assertEqual(get_value(job, "worksite_fixed_by"), "impossible")
        self.assertTrue(get_value(job, "fixed"))

    def test_one_na(self):
        job = successes[successes["CASE_NUMBER"] == "H-300-20161-638892"]
        self.assertEqual(get_value(job, "HOUSING_CITY"), "Clear Lake")
        self.assertEqual(get_value(job, "housing accuracy"), 0.8)
        self.assertEqual(get_value(job, "housing accuracy type"), "range_interpolation")
        self.assertTrue(pd.isnull(get_value(job, "housing_fixed_by")))
        self.assertEqual(get_value(job, "WORKSITE_ADDRESS"), "124 Raymond Ave")
        self.assertEqual(get_value(job, "WORKSITE_CITY"), "Poughkeepsie")
        self.assertEqual(get_value(job, "worksite accuracy"), 1)
        self.assertEqual(get_value(job, "worksite accuracy type"), "rooftop")
        self.assertEqual(get_value(job, "worksite_fixed_by"), "address")
        self.assertTrue(get_value(job, "fixed"))

    def test_one_na_but_needs_fixing(self):
        job = failures[failures["CASE_NUMBER"] == "H-300-20214-746991"]
        self.assertEqual(get_value(job, "HOUSING_CITY"), "Yuma")
        self.assertEqual(get_value(job, "housing accuracy"), 1)
        self.assertEqual(get_value(job, "housing accuracy type"), "rooftop")
        self.assertEqual(get_value(job, "housing_fixed_by"), "coordinates")
        self.assertEqual(get_value(job, "WORKSITE_ADDRESS"), "A Loghry: hal-1-3, hal-1-4: County 15th Street & Avenue A (W)")
        self.assertEqual(get_value(job, "worksite accuracy"), 1)
        self.assertEqual(get_value(job, "worksite accuracy type"), "place")
        self.assertEqual(get_value(job, "worksite_fixed_by"), "failed")
        self.assertEqual(get_value(job, "fixed"), False)

    def test_works_for_dolH_row(self):
        job = housing_successes[housing_successes["CASE_NUMBER"] == "H-300-19296-103454"]
        self.assertEqual(get_value(job, "housing_fixed_by"), "address")
        self.assertEqual(get_value(job, "HOUSING_ADDRESS_LOCATION"), "124 Raymond Ave")
        self.assertEqual(get_value(job, "HOUSING_CITY"), "Poughkeepsie")
        self.assertEqual(get_value(job, "HOUSING_POSTAL_CODE"), 12604)
        self.assertEqual(get_value(job, "HOUSING_STATE"), "NY")
        self.assertEqual(get_value(job, "housing accuracy"), 1)
        self.assertEqual(get_value(job, "housing accuracy type"), "rooftop")
        self.assertEqual(get_value(job, "housing_lat"), 41.686518)
        self.assertEqual(get_value(job, "housing_long"), -73.897729)
        self.assertEqual(get_value(job, "housing_fixed_by"), "address")
        self.assertTrue(get_value(job, "fixed"))

    def test_h2b_doesnt_need_housing(self):
        job = successes[successes["CASE_NUMBER"] == "H-400-2111"]
        self.assertTrue(pd.isnull(get_value(job, "HOUSING_CITY")))
        self.assertTrue(pd.isnull(get_value(job, "housing accuracy")))
        self.assertTrue(pd.isnull(get_value(job, "housing_fixed_by")))
        self.assertEqual(get_value(job, "WORKSITE_CITY"), "Somerset County")
        self.assertEqual(get_value(job, "worksite accuracy"), 1)
        self.assertEqual(get_value(job, "worksite accuracy type"), "rooftop")
        self.assertEqual(get_value(job, "worksite_lat"), 3)
        self.assertEqual(get_value(job, "worksite_long"), 5)
        self.assertEqual(get_value(job, "worksite_fixed_by"), "coordinates")
        self.assertTrue(get_value(job, "fixed"))

    def test_fixed_but_failed(self):
        job = failures[failures["CASE_NUMBER"] == "H-300-20150-612472"]
        self.assertEqual(get_value(job, "HOUSING_ADDRESS_LOCATION"), "W1026 Buttercup Court")
        self.assertEqual(get_value(job, "housing accuracy type"), "rooftop")
        self.assertEqual(get_value(job, "housing_long"), -88.932913)
        self.assertTrue(pd.isnull(get_value(job, "housing_fixed_by")))
        self.assertEqual(get_value(job, "WORKSITE_ADDRESS"), "4 Rainbow Rd")
        self.assertEqual(get_value(job, "WORKSITE_CITY"), "Argentina")
        self.assertEqual(get_value(job, "worksite accuracy"), 0.18)
        self.assertEqual(get_value(job, "worksite accuracy type"), "place")
        self.assertEqual(get_value(job, "worksite_lat"), 40.744824)
        self.assertEqual(get_value(job, "worksite_long"), -73.948749)
        self.assertEqual(get_value(job, "worksite_fixed_by"), "failed")
        self.assertEqual(get_value(job, "fixed"), False)

    def test_lengths_and_no_dups(self):
        self.assertEqual(len(successes), 7)
        self.assertEqual(len(failures), 2)
        self.assertEqual(len(housing_successes), 1)
        self.assertTrue(all(has_no_dups(successes, failures)))

    def test_columns(self):
        housing_columns, accurate_columns, inaccurate_columns = housing_successes.columns, successes.columns, failures.columns
        self.assertTrue("HOUSING_ADDRESS_LOCATION" in housing_columns)
        self.assertTrue("fixed" in housing_columns)
        self.assertTrue("housing_fixed_by" in housing_columns)
        self.assertFalse("WORKSITE_STATE" in housing_columns)
        self.assertFalse("worksite accuracy type" in housing_columns)
        self.assertTrue("HOUSING_POSTAL_CODE" in accurate_columns)
        self.assertTrue("worksite accuracy type" in accurate_columns)
        self.assertTrue("HOUSING_ADDRESS_LOCATION" in inaccurate_columns)
        self.assertTrue("fixed" in inaccurate_columns)
        self.assertTrue("housing_fixed_by" in inaccurate_columns)
        self.assertTrue("WORKSITE_STATE" in inaccurate_columns)
        self.assertTrue("worksite accuracy type" in inaccurate_columns)
        self.assertTrue("HOUSING_POSTAL_CODE" in inaccurate_columns)
        self.assertTrue("worksite accuracy type" in inaccurate_columns)

unittest.main(verbosity=2)
# unittest.main()
