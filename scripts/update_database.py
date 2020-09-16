import requests
import helpers
from column_name_mappings import column_name_mappings
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from geocodio import GeocodioClient
database_connection_string, geocodio_api_key, most_recent_run_url, date_of_run_url, _, _, _ = helpers.get_secret_variables()
engine, client = create_engine(database_connection_string), GeocodioClient(geocodio_api_key)

def update_database():
    latest_jobs = requests.get(most_recent_run_url).json()
    if not latest_jobs:
        myprint("No new jobs.")
        return
    myprint(f"There are {len(latest_jobs)} new jobs.")

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

    # add necessary columns to job
    def add_necessary_columns(job):
        # add source and date of run column
        job["Source"], job["table"] = "Apify", "central"
        job["Date of run"] = requests.get(date_of_run_url).json()["data"]["finishedAt"]
        # check if job is h2a
        if helpers.h2a_or_h2b(job) == "H-2A":
            job["Visa type"] = "H-2A"
            zip_code_columns = ["EMPLOYER_POSTAL_CODE", "WORKSITE_POSTAL_CODE",  "Place of Employment Info/Postal Code", "HOUSING_POSTAL_CODE"]
            job["TOTAL_WORKERS_NEEDED"] = job["TOTAL_WORKERS_H-2A_REQUESTED"]
            workers_needed, occupancy = job["TOTAL_WORKERS_NEEDED"], job["TOTAL_OCCUPANCY"]
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
            fixed_zip_code = helpers.fix_zip_code(job[column])
            job[column] = fixed_zip_code
        return job

    # parse each job, add all columns to each job, append this to raw scraper data and push back to postgres
    parsed_jobs = [parse(job) for job in latest_jobs]
    full_jobs = [add_necessary_columns(job) for job in parsed_jobs]
    full_jobs_df = pd.DataFrame(full_jobs)
    # raw_scraper_jobs = pd.read_sql('raw_scraper_jobs', con=engine)
    # raw_new_and_old_jobs = full_jobs_df.append(raw_scraper_jobs, ignore_index=True, sort=True)
    full_jobs_df.to_sql("raw_scraper_jobs", engine, if_exists="append", index=False, dtype=helpers.column_types)

    # geocode, split by accuracy, get old data, merge old with new data, sort data
    new_accurate_jobs, new_inaccurate_jobs = helpers.geocode_and_split_by_accuracy(full_jobs_df)
    job_central, low_accuracies = pd.read_sql("job_central", con=engine), pd.read_sql("low_accuracies", con=engine)
    accurate_jobs, inaccurate_jobs = helpers.merge_all_data(new_accurate_jobs, new_inaccurate_jobs, job_central, low_accuracies)

    accurate_jobs["RECEIVED_DATE"] = pd.to_datetime(accurate_jobs["RECEIVED_DATE"])
    inaccurate_jobs["RECEIVED_DATE"] = pd.to_datetime(inaccurate_jobs["RECEIVED_DATE"])
    accurate_jobs, inaccurate_jobs = helpers.sort_df_by_date(accurate_jobs), helpers.sort_df_by_date(inaccurate_jobs)

    # send updated data back to postgres
    accurate_jobs.to_sql('job_central', engine, if_exists='replace', index=False, dtype=helpers.column_types)
    inaccurate_jobs.to_sql('low_accuracies', engine, if_exists='replace', index=False, dtype=helpers.column_types)

if __name__ == "__main__":
   update_database()
