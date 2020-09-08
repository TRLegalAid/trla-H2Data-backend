import os
import pandas as pd
import sqlalchemy
import helpers
from sqlalchemy import create_engine
database_connection_string = helpers.get_secret_variables()[0]
engine = create_engine(database_connection_string)

def geocode_manage_split(df):
    renaming_info_dict = {"Section A": "Job Info", "Section C": "Place of Employment Info", "Section D":"Housing Info"}
    column_names_dict = {}
    for column in df.columns:
        for key in renaming_info_dict:
            if key in column:
                if column == "Section C/Address/Location":
                    column_names_dict[column] = "Place of Employment Info/Address/Location"
                else:
                    column_names_dict[column] = renaming_info_dict[key] + "/" + column.split("/")[1]
    df = df.rename(columns=column_names_dict)
    df = df.drop(columns=["Telephone number"])
    df = helpers.rename_columns(df)
    df["Source"] = "Apify"
    df['Visa type'] = df.apply(lambda job: helpers.h2a_or_h2b(job), axis=1)
    def fix_case_number_if_needed(job):
        if (job["CASE_NUMBER"][0] == "3") or (job["CASE_NUMBER"][0] == "4"):
            return "H-" + job["CASE_NUMBER"]
        else:
            return job["CASE_NUMBER"]
    df["CASE_NUMBER"] = df.apply(lambda job: fix_case_number_if_needed(job), axis=1)
    def get_num_workers(job):
        if job["Visa type"] == "H-2A":
            return job["TOTAL_WORKERS_H-2A_REQUESTED"]
        else:
            return job["Number of Workers Requested H-2B"]
    df['TOTAL_WORKERS_NEEDED'] = df.apply(lambda job: get_num_workers(job), axis=1)
    df = helpers.fix_zip_code_columns(df, ["EMPLOYER_POSTAL_CODE", "WORKSITE_POSTAL_CODE",  "Place of Employment Info/Postal Code", "HOUSING_POSTAL_CODE"])
    raw_scraper_jobs = df
    df = df.drop_duplicates(subset='CASE_NUMBER', keep="last")

    i = len(df.columns)
    df.insert(i, "fixed", None)
    df.insert(i, "worksite_fixed_by", None)
    df.insert(i, "housing_fixed_by", None)
    df.insert(i, "notes", "")
    df.insert(i, "table", "central")

    accurate_jobs, inaccurate_jobs = helpers.geocode_and_split_by_accuracy(df)
    accurate_jobs, inaccurate_jobs = helpers.sort_df_by_date(accurate_jobs), helpers.sort_df_by_date(inaccurate_jobs)
    return accurate_jobs, inaccurate_jobs, raw_scraper_jobs

def send_to_postgres():
    # scraper_jobs = pd.read_excel(os.path.join(os.getcwd(), '..', 'excel_files/scraper_data.xlsx'))
    scraper_jobs = pd.read_excel(os.path.join(os.getcwd(), '..', 'excel_files/all_scraper_data.xlsx'), nrows=3811)

    accurate_jobs, inaccurate_jobs, raw_scraper_jobs = geocode_manage_split(scraper_jobs)
    accurate_jobs.to_sql('job_central', engine, if_exists='replace', index=False, dtype=helpers.column_types)
    inaccurate_jobs.to_sql('low_accuracies', engine, if_exists='replace', index=False, dtype=helpers.column_types)
    raw_scraper_jobs.to_sql('raw_scraper_jobs', engine, if_exists='replace', index=False, dtype=helpers.column_types)

if __name__ == "__main__":
   send_to_postgres()
