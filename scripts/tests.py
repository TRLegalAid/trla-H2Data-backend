from populate_database import populate_database
from helpers import test
from colorama import Fore, Style
import os
import pandas as pd
import sqlalchemy
import helpers
from sqlalchemy import create_engine
from merge_dol import merge_dol
import time
database_connection_string = helpers.get_secret_variables()[0]
engine = create_engine(database_connection_string)
all_tests = []

def run_test(value, should_be, name):
    all_tests.append(test(value, should_be, name=name))

job_central = "job_central_test"
low_accuracies = "low_accuracies_test"
additional_housing = "additional_housing_test"
raw_scraper_jobs = "raw_scraper_jobs_test"
all_table_names = [job_central, low_accuracies, additional_housing, raw_scraper_jobs]

def get_table(table):
    try:
        return pd.read_sql(table, con=engine)
    except:
        return pd.DataFrame

def get_all_tables():
    job_central_df = get_table(job_central)
    time.sleep(1)
    low_accuracies_df = get_table(low_accuracies)
    time.sleep(1)
    additional_housing_df = get_table(additional_housing)
    time.sleep(1)
    raw_scraper_jobs_df = get_table(raw_scraper_jobs)
    return job_central_df, low_accuracies_df, additional_housing_df, raw_scraper_jobs_df

scraper_data = pd.read_excel(os.path.join(os.getcwd(), '..',  "excel_files/scraper_data.xlsx"))

populate_database(scraper_data, job_central, low_accuracies, raw_scraper_jobs)
job_central, low_accuracies, additional_housing, raw_scraper_jobs = get_all_tables()

run_test(len(job_central), 12, name="Job central table is the right length")
run_test(len(low_accuracies), 2, name="Low accuracies table is the right length.")
run_test(len(raw_scraper_jobs), 14, name="Raw scraper jobs is the right length.")

# dol_jobs = pd.read_excel(os.path.join(os.getcwd(), '..', 'excel_files/dol_data.xlsx'), converters={'ATTORNEY_AGENT_PHONE':str,'PHONE_TO_APPLY':str})
# merge_dol(dol_jobs, job_central, low_accuracies)
# job_central, low_accuracies, additional_housing, raw_scraper_jobs = get_all_tables()
#
# run_test(len(job_central), 19, name="Job central table is the right length")
# run_test(len(low_accuracies), 9, name="Low accuracies table is the right length.")
# run_test(len(raw_scraper_jobs), 14, name="Raw scraper jobs is the right length.")
#
# number_of_tests_ran = len(all_tests)
# if all(all_tests):
#     print(Fore.GREEN + "All tests passed!")
#     print(Style.RESET_ALL)
# else:
#     print(Fore.RED + f"{all_tests.count(False)} out of {number_of_tests_ran} tests failed - see output above.")
#     print(Style.RESET_ALL)
