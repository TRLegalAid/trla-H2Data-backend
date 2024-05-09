
import os
import pandas as pd
import helpers
from datetime import datetime
import pytz
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from dotenv import load_dotenv

from h2b_json_to_apify_mapping import key_mapping_h2b
from apify_data_fields import apify_data_fields
from column_name_mappings import column_name_mappings

load_dotenv()

engine = helpers.get_database_engine(force_cloud=True)
json_engine = create_engine(os.getenv("JSON_DATABASE_URL"))


# TO DO:
# [x] Confirm the JSON H2B <> TRLA mapping
# [] Confirm the JSON H2A <> TRLA mapping
# [x] Map the JSON H2B data to the full set of fields returned by Apify scraper
# [x] Handle fields: Experience Required, Job Order Link, Job Summary Link, Source, Booleans, Dates
# [x] Test run

# 1) Identify jobs in JSON that are not in TRLA based on: JSON date_acceptance_ltr_issued and TRLA "Date of run" 

json_h2b = pd.read_sql(f"""SELECT case_number, date_acceptance_ltr_issued_ymd FROM h2b WHERE date_acceptance_ltr_issued_ymd >='2024-01-03'""", con=json_engine)

trla_h2b = pd.read_sql(f"""SELECT DISTINCT "CASE_NUMBER", "Date of run" FROM raw_scraper_jobs WHERE "Date of run" >= '2024-01-03' AND "Visa type" = 'H-2B'""", con=engine)

merged = pd.merge(json_h2b, trla_h2b, left_on='case_number', right_on='CASE_NUMBER', how='left', indicator=True)

missing_h2b = merged[merged['_merge'] == 'left_only'].drop(columns='_merge')

case_numbers = tuple(missing_h2b['case_number'])



# 2) Given the list of missing H2B jobs, pull the data from JSON h2b table that is needed to update the TRLA database

h2b_field_names = list(key_mapping_h2b.values())

json_h2b_read_query = """
SELECT {}
FROM h2b
WHERE case_number IN ({});
""".format(", ".join(h2b_field_names), ", ".join(["%s"] * len(case_numbers)))

json_h2b_for_patch = pd.read_sql(json_h2b_read_query, con=json_engine, params=case_numbers)


# What's the full set of columns that the original update_database.py script expects?
# What values are typically there in the other columns for h2b records?

jobs_for_patch = pd.DataFrame(columns=apify_data_fields.keys())


# for every pair of columns in our key_mapping: 
# set the target column of our new df to the value of the source column in every row in the json_h2b_for_patch df
# if the target column is not listed in our key mapping, set the value in the new df to None

for target_col, source_col in key_mapping_h2b.items(): 
    jobs_for_patch[target_col] = json_h2b_for_patch.apply(lambda row: row[source_col], axis=1)

for col in jobs_for_patch.columns:
    if col not in key_mapping_h2b.keys():
        jobs_for_patch[col] = None

latest_jobs = jobs_for_patch.to_dict('records')



def parse(job):
        
        column_mappings_dict = column_name_mappings
        columns_names_dict = {"Section A": "Job Info", "Section C": "Place of Employment Info", "Section D":"Housing Info"}
        parsed_job = {}
        for key in job:
            if "Section A" in key or "Section C" in key or "Section D" in key:
                section = key.split("/")[0]
                key_name = key.replace(section, columns_names_dict[section])
                if key_name in column_mappings_dict:
                    parsed_job[column_mappings_dict[key_name]] = job[key]
                else:
                    parsed_job[key_name] = job[key]
            else:
                if key in column_mappings_dict:
                    parsed_job[column_mappings_dict[key]] = job[key]
                else:
                    parsed_job[key] = job[key]
    
        return parsed_job


us_central_timezone = pytz.timezone('US/Central')
now = datetime.now(us_central_timezone)
date_of_run = now.strftime("%Y-%m-%d %H:%M:%S")


# adds calculated fields to the dictionary job
def add_necessary_columns(job):
        
        # add source and date of run column
        job["Source"], job["table"] = "JSON", "central"
        job["Date of run"] = date_of_run

        case_number = job['CASE_NUMBER']

        job['Job Order Link'] = f"https://api.seasonaljobs.dol.gov/job-order/{case_number}"
        job['Job Summary Link'] = f"https://seasonaljobs.dol.gov/jobs/{case_number}"


        if helpers.h2a_or_h2b(job) == "H-2A":
            job["Visa type"] = "H-2A"
            zip_code_columns = ["EMPLOYER_POSTAL_CODE", "WORKSITE_POSTAL_CODE",  "Place of Employment Info/Postal Code", "HOUSING_POSTAL_CODE"]

            if "TOTAL_WORKERS_H-2A_REQUESTED" in job:
                job["TOTAL_WORKERS_NEEDED"] = job["TOTAL_WORKERS_H-2A_REQUESTED"]

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


# parse each job, add necessary columns to each job, drop duplicates case numbers
parsed_jobs = [parse(job) for job in latest_jobs]
full_jobs = [add_necessary_columns(job) for job in parsed_jobs]
full_jobs_df = pd.DataFrame(full_jobs).drop_duplicates(subset="CASE_NUMBER", keep="last")

# convert date columns to type datetime
date_columns = ["RECEIVED_DATE", "EMPLOYMENT_END_DATE", "EMPLOYMENT_BEGIN_DATE", "Date of run"]
for column in date_columns:
    full_jobs_df[column] = pd.to_datetime(full_jobs_df[column], errors='coerce')

# Convert OTHER_WORKSITE_LOCATION to boolean
full_jobs_df['OTHER_WORKSITE_LOCATION'] = full_jobs_df['OTHER_WORKSITE_LOCATION'].astype(bool)



# Add data as is to raw_scraper_jobs
full_raw_jobs = full_jobs_df.drop(columns=["table"])
full_raw_jobs.to_sql("raw_scraper_jobs", engine, if_exists="append", index=False)
print(f"Added {len(full_raw_jobs)} jobs to raw_scraper_jobs table in TRLA seasonal jobs PostgreSQL database.")

# Geocode, split by accuracy, merge existing data with new data
new_accurate_jobs, new_inaccurate_jobs = helpers.geocode_and_split_by_accuracy(full_jobs_df)
helpers.merge_all_data(new_accurate_jobs, new_inaccurate_jobs)




# if __name__ == "__main__":
#    update_database()
