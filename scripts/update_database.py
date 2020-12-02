import os
import requests
import helpers
from helpers import myprint, get_database_engine
from column_name_mappings import column_name_mappings
import pandas as pd
from geocodio import GeocodioClient
from dotenv import load_dotenv
load_dotenv()

geocodio_api_key, most_recent_run_url, date_of_run_url = os.getenv("GEOCODIO_API_KEY"), os.getenv("MOST_RECENT_RUN_URL"), os.getenv("DATE_OF_RUN_URL")
engine, client = get_database_engine(force_cloud=False), GeocodioClient(geocodio_api_key)

def update_database():
    latest_jobs = requests.get(most_recent_run_url).json()

    # use these two lines if you're updating using a local csv file
    # latest_jobs = pd.read_csv("Since_10_23_to_10_29.csv", encoding='unicode_escape').drop(columns=["Unnamed: 0"])
    # latest_jobs = latest_jobs.to_dict('records')

    if not latest_jobs:
        myprint("No new jobs.")
        return
    myprint(f"There are {len(latest_jobs)} new jobs.")

    # use this version of parse function if using a local csv file
    # def parse(job):
    #     column_mappings_dict = column_name_mappings
    #     columns_names_dict = {"Section A": "Job Info", "Section C": "Place of Employment Info", "Section D":"Housing Info"}
    #     parsed_job = {}
    #     for key in job:
    #         if "Section A" in key or "Section C" in key or "Section D" in key:
    #             section = key.split("/")[0]
    #             key_name = key.replace(section, columns_names_dict[section])
    #             if key_name in column_mappings_dict:
    #                 parsed_job[column_mappings_dict[key_name]] = job[key]
    #             else:
    #                 parsed_job[key_name] = job[key]
    #         else:
    #             if key in column_mappings_dict:
    #                 parsed_job[column_mappings_dict[key]] = job[key]
    #             else:
    #                 parsed_job[key] = job[key]
    #
    #     return parsed_job


    def parse(job):
        column_mappings_dict = column_name_mappings
        columns_names_dict = {"Section A": "Job Info", "Section C": "Place of Employment Info", "Section D":"Housing Info"}
        parsed_job = {}
        for key in job:
            if key not in ["Section D", "Section C", "Section A"]:
                if key in column_mappings_dict:
                    parsed_job[column_mappings_dict[key]] = job[key]
                else:
                    parsed_job[key] = job[key]
            else:
                inner_dict = job[key]
                for inner_key in inner_dict:
                    key_name = columns_names_dict[key] + "/" + inner_key
                    if key_name in column_mappings_dict:
                        parsed_job[column_mappings_dict[key_name]] = inner_dict[inner_key]
                    else:
                        parsed_job[key_name] = inner_dict[inner_key]
        return parsed_job

    date_of_run = requests.get(date_of_run_url).json()["data"]["finishedAt"]

    def add_necessary_columns(job):
        # add source and date of run column
        job["Source"], job["table"] = "Apify", "central"
        job["Date of run"] = date_of_run
        # check if job is h2a
        if helpers.h2a_or_h2b(job) == "H-2A":
            job["Visa type"] = "H-2A"
            zip_code_columns = ["EMPLOYER_POSTAL_CODE", "WORKSITE_POSTAL_CODE",  "Place of Employment Info/Postal Code", "HOUSING_POSTAL_CODE"]
            if "TOTAL_WORKERS_H-2A_REQUESTED" in job:
                job["TOTAL_WORKERS_NEEDED"] = job["TOTAL_WORKERS_H-2A_REQUESTED"]
            workers_needed, occupancy = job["TOTAL_WORKERS_NEEDED"], job.get("TOTAL_OCCUPANCY", None)
            if workers_needed and occupancy:
                if workers_needed > occupancy:
                    job["W to H Ratio"] = "W>H"
                elif workers_needed < occupancy:
                    job["W to H Ratio"] = "W<H"
                else:
                    job["W to H Ratio"] = "W=H"
            if job["CASE_NUMBER"][0] == "3":
                job["CASE_NUMBER"] = "H-" + job["CASE_NUMBER"]
        # check if job is h2b
        elif helpers.h2a_or_h2b(job) == "H-2B":
            job["Visa type"] = "H-2B"
            zip_code_columns = ["EMPLOYER_POSTAL_CODE", "WORKSITE_POSTAL_CODE"]
        else:
            job["Visa type"], zip_code_columns = "", []
        # fix zip code columns
        for column in zip_code_columns:
            if column in job:
                fixed_zip_code = helpers.fix_zip_code(job[column])
                job[column] = fixed_zip_code
        return job

    # parse each job, add all columns to each job, append this to raw scraper data and push back to postgres
    parsed_jobs = [parse(job) for job in latest_jobs]
    full_jobs = [add_necessary_columns(job) for job in parsed_jobs]
    full_jobs_df = pd.DataFrame(full_jobs).drop_duplicates(subset="CASE_NUMBER", keep="last")

    date_columns = ["RECEIVED_DATE", "EMPLOYMENT_END_DATE", "EMPLOYMENT_BEGIN_DATE", "Date of run"]
    for column in date_columns:
        full_jobs_df[column] = pd.to_datetime(full_jobs_df[column], errors='coerce')

    full_raw_jobs = full_jobs_df.drop(columns=["table"])
    full_raw_jobs.to_sql("raw_scraper_jobs", engine, if_exists="append", index=False)
    myprint("Uploaded raw scraper jobs to PostgreSQL")

    # geocode, split by accuracy, get old data, merge old with new data, sort data
    new_accurate_jobs, new_inaccurate_jobs = helpers.geocode_and_split_by_accuracy(full_jobs_df)

    helpers.merge_all_data(new_accurate_jobs, new_inaccurate_jobs)

if __name__ == "__main__":
   update_database()
