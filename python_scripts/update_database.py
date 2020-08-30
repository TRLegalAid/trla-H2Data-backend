import requests
import helpers
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from geocodio import GeocodioClient
database_connection_string, geocodio_api_key = helpers.get_secret_variables()
engine, client = create_engine(database_connection_string), GeocodioClient(geocodio_api_key)

def update_database():

    # most recent run - https://api.apify.com/v2/acts/eytaog~apify-dol-actor-latest/runs/last/dataset/items?token=ftLRsXTA25gFTaCvcpnebavKw
    latest_jobs = requests.get("https://api.apify.com/v2/datasets/g0awKuoXUdCsY2oBa/items?format=json&clean=1").json()[:11]

    # parse job so it's not a nested dictionary
    def parse(job):
        column_mappings_dict = helpers.get_column_mappings_dictionary()
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
        job["Date of run"] = requests.get("https://api.apify.com/v2/acts/eytaog~apify-dol-actor-latest/runs/last?token=ftLRsXTA25gFTaCvcpnebavKw").json()["data"]["finishedAt"]
        # check if job is h2a
        if job["CASE_NUMBER"][2] == "3":
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
        # check if job is h2b
        elif job["CASE_NUMBER"][2] == "4":
            job["Visa type"] = "H-2B"
            zip_code_columns = ["EMPLOYER_POSTAL_CODE", "WORKSITE_POSTAL_CODE"]
            # job["TOTAL_WORKERS_NEEDED"] = job["Job Info/Workers Needed H-2A"]
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
    raw_scraper_jobs = pd.read_sql('raw_scraper_jobs', con=engine)
    raw_new_and_old_jobs = full_jobs_df.append(raw_scraper_jobs, ignore_index=True, sort=True)
    raw_new_and_old_jobs = raw_new_and_old_jobs.drop_duplicates(subset='CASE_NUMBER', keep="last")
    raw_new_and_old_jobs.to_sql("raw_scraper_jobs", engine, if_exists="replace", index=False, dtype=helpers.column_types)

    # geocode, split by accuracy, merge old with new data
    new_accurate_jobs, new_inaccurate_jobs = helpers.geocode_and_split_by_accuracy(full_jobs_df)
    job_central = pd.read_sql("job_central", con=engine)
    low_accuracies = pd.read_sql("low_accuracies", con=engine)
    accurate_jobs, inaccurate_jobs = helpers.merge_all_data(new_accurate_jobs, new_inaccurate_jobs, job_central, low_accuracies)

    # send updated data back to postgres
    accurate_jobs.to_sql('job_central', engine, if_exists='replace', index=False, dtype=helpers.column_types)
    inaccurate_jobs.to_sql('low_accuracies', engine, if_exists='replace', index=False, dtype=helpers.column_types)

update_database()
