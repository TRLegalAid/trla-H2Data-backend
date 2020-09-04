from populate_database import populate_database
from helpers import test, myprint
from colorama import Fore, Style
import os
import pandas as pd
import sqlalchemy
import helpers
from sqlalchemy import create_engine
import time
database_connection_string = helpers.get_secret_variables()[0]
engine = create_engine(database_connection_string)
all_tests = []
def run_test(value, should_be, name):
    all_tests.append(test(value, should_be, name=name))


def test_populate_database():
    myprint("Testing populate_database....")
    scraper_data = pd.read_excel(os.path.join(os.getcwd(), '..',  "excel_files/scraper_data.xlsx"))
    accurates, inaccurates, raw_scrapers = populate_database(scraper_data)

    run_test(len(accurates), 12, name="Job central table is the right length")
    run_test(len(inaccurates), 2, name="Low accuracies table is the right length")
    run_test(len(raw_scrapers), 15, name="Raw scraper jobs is the right length")
    run_test((accurates["worksite accuracy"] >= 0.8).all(), True, name="All worksite accuracies are high enough.")

    accurates_h2a = accurates[accurates["Visa type"] == "H-2A"]
    run_test((accurates_h2a["housing accuracy"] >= 0.8).all(), True, name="All H-2A housing accuracies are high enough.")

    inaccurates_h2a = inaccurates[inaccurates["Visa type"] == "H-2A"]
    inaccurate_conditions_h2a = ((inaccurates["housing accuracy"].isnull()) | (inaccurates["worksite accuracy"].isnull()) | (inaccurates["housing accuracy"] < 0.8) | (inaccurates["worksite accuracy"] < 0.8) | (inaccurates["housing accuracy type"].isin(helpers.bad_accuracy_types)) | (inaccurates["worksite accuracy type"].isin(helpers.bad_accuracy_types)))
    run_test(inaccurate_conditions_h2a.all(), True, name="All H-2A's in inaccurates are actually inaccurate")

    inaccurates_h2b = inaccurates[inaccurates["Visa type"] == "H-2B"]
    inaccurate_conditions_h2b = ((inaccurates["worksite accuracy"].isnull()) | (inaccurates["worksite accuracy"] < 0.8) | (inaccurates["worksite accuracy type"].isin(helpers.bad_accuracy_types)))
    run_test(inaccurate_conditions_h2b.all(), True, name="All H-2B's in inaccurates are actually inaccurate")
    # could also test datatypes, other columns

def test_add_housing():
    myprint("Testing add_housing....")

# test_populate_database()





num_tests, num_false = len(all_tests), all_tests.count(False)
if num_false == 0:
    print(Fore.GREEN + "\nAll tests passed!" + Style.RESET_ALL)
else:
    print(Fore.RED + f"\n{num_false} out of {num_tests} failed" + Style.RESET_ALL)
















#
# job_central_str, low_accuracies_str, additional_housing_str, raw_scraper_jobs_str, = "job_central_test", "low_accuracies_test", "additional_housing_test", "raw_scraper_jobs_test"
# all_table_names = [job_central, low_accuracies, additional_housing, raw_scraper_jobs]
#
# def get_table(table):
#     try:
#         return pd.read_sql(table, con=engine)
#     except:
#         return pd.DataFrame
#
# def get_all_tables():
#     job_central_df = get_table(job_central)
#     time.sleep(1)
#     low_accuracies_df = get_table(low_accuracies)
#     time.sleep(1)
#     additional_housing_df = get_table(additional_housing)
#     time.sleep(1)
#     raw_scraper_jobs_df = get_table(raw_scraper_jobs)
#     return job_central_df, low_accuracies_df, additional_housing_df, raw_scraper_jobs_df
