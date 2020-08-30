from math import isnan
import os
import pandas as pd
from geocodio import GeocodioClient
import requests
import logging
import sqlalchemy
pd.options.mode.chained_assignment = None
logger = logging.Logger('catch_all')
bad_accuracy_types = ["place", "state", "street_center"]
column_types = {
    "fixed": sqlalchemy.types.Boolean, "Experience Required": sqlalchemy.types.Boolean, "Multiple Worksites": sqlalchemy.types.Boolean,
    "Date of run": sqlalchemy.types.DateTime, "RECEIVED_DATE": sqlalchemy.types.DateTime, "EMPLOYMENT_BEGIN_DATE": sqlalchemy.types.DateTime,
    "EMPLOYMENT_END_DATE": sqlalchemy.types.DateTime, "HOUSING_POSTAL_CODE": sqlalchemy.types.Text, "Job Info/Workers Needed Total": sqlalchemy.types.Integer,
    "PHONE_TO_APPLY": sqlalchemy.types.Text, "Place of Employment Info/Postal Code": sqlalchemy.types.Text, "TOTAL_OCCUPANCY": sqlalchemy.types.Integer,
    "TOTAL_UNITS": sqlalchemy.types.Integer, "TOTAL_WORKERS_H-2A_REQUESTED": sqlalchemy.types.Integer, "TOTAL_WORKERS_NEEDED": sqlalchemy.types.Integer,
    "WORKSITE_POSTAL_CODE": sqlalchemy.types.Text, "ATTORNEY_AGENT_PHONE": sqlalchemy.types.Text
}

housing_address_columns = ["HOUSING_ADDRESS_LOCATION", "HOUSING_CITY", "HOUSING_STATE", "HOUSING_POSTAL_CODE", "housing coordinates", "housing accuracy", "housing accuracy type", "housing_fixed_by", "fixed"]
worksite_address_columns = ["WORKSITE_ADDRESS", "WORKSITE_CITY", "WORKSITE_STATE", "WORKSITE_POSTAL_CODE", "worksite coordinates", "worksite accuracy", "worksite accuracy type", "worksite_fixed_by", "fixed"]


# function for printing dictionary
def prettier(dictionary):
    for key in dictionary:
        print(key, ": ", dictionary[key])

def get_secret_variables():
    # LOCAL_DEV is an environemnt variable that I set to be "true" on my mac and "false" in the heroku config variables
    if os.getenv("LOCAL_DEV") == "true":
        import secret_variables
        return secret_variables.DATABASE_URL, secret_variables.GEOCODIO_API_KEY
    return os.getenv("DATABASE_URL"), os.getenv("GEOCODIO_API_KEY")
geocodio_api_key = get_secret_variables()[1]
client = GeocodioClient(geocodio_api_key)


def create_address_from(address, city, state, zip):
    try:
        return address + ", " + city + " " + state + " " + str(zip)
    except:
        return ""

def geocode_table(df, worksite_or_housing):
    print(f"Geocoding {worksite_or_housing}...")

    if worksite_or_housing == "worksite":
        geocoding_type = "worksite"
        addresses = df.apply(lambda job: create_address_from(job["WORKSITE_ADDRESS"], job["WORKSITE_CITY"], job["WORKSITE_STATE"], job["WORKSITE_POSTAL_CODE"]), axis=1).tolist()
    elif worksite_or_housing == "housing":
        geocoding_type = "housing"
        addresses = df.apply(lambda job: create_address_from(job["HOUSING_ADDRESS_LOCATION"], job["HOUSING_CITY"], job["HOUSING_STATE"], job["HOUSING_POSTAL_CODE"]), axis=1).tolist()
    elif worksite_or_housing == "housing addendum":
        geocoding_type = "housing"
        addresses = df.apply(lambda job: create_address_from(job["PHYSICAL_LOCATION_ADDRESS1"], job["PHYSICAL_LOCATION_CITY"], job["PHYSICAL_LOCATION_STATE"], job["PHYSICAL_LOCATION_POSTAL_CODE"]), axis=1).tolist()
    else:
        print("`worksite_or_housing` must be either `worksite` or `housing` or `housing addendum`")
        return

    geocoding_results = client.geocode(addresses)
    coordinates = [result.coords  for result in geocoding_results]
    accuracies = [result.accuracy for result in geocoding_results]
    accuracy_types = [None if not result["results"] else result["results"][0]["accuracy_type"] for result in geocoding_results]

    df[f"{geocoding_type} coordinates"] = coordinates
    df[f"{geocoding_type} accuracy"] = accuracies
    df[f"{geocoding_type} accuracy type"] = accuracy_types
    print(f"Finished geocoding {worksite_or_housing}. \n")


def fix_zip_code(zip_code):
    if isinstance(zip_code, str):
        return ("0" * (5 - len(zip_code))) + zip_code
    elif zip_code == None or isnan(zip_code):
        return None
    else:
        zip_code = str(int(zip_code))
        return ("0" * (5 - len(zip_code))) + zip_code

def fix_zip_code_columns(df, columns):
    for column in columns:
        df[column] = df.apply(lambda job: fix_zip_code(job[column]), axis=1)

def is_accurate(job):
    our_states = ["texas", "kentucky", "tennessee", "arkansas", "louisiana", "mississippi", "alabama"]
    # if (job["WORKSITE_STATE"].lower() not in our_states):
    #     return True
    if job["table"] == "central":
        if job["Visa type"] == "H-2A":
            return not ((job["worksite coordinates"] == None) or (job["housing coordinates"] == None) or (job["worksite accuracy"] < 0.8) or (job["housing accuracy"] < 0.8) or (job["worksite accuracy type"] in bad_accuracy_types) or (job["housing accuracy type"] in bad_accuracy_types))
        elif job["Visa type"] == "H-2B":
            return not ((job["worksite coordinates"] == None) or (job["worksite accuracy"] < 0.8) or (job["worksite accuracy type"] in bad_accuracy_types))
        else:
            print(f"the `Visa type` column of this job - case number {job['CASE_NUMBER']} -  was neither `H-2a` nor `H-2B`")
            return False

    elif job["table"] == "dol_h":
        return not ((job["housing coordinates"] == None) or (job["housing accuracy"] < 0.8) or (job["housing accuracy type"] in bad_accuracy_types))

    else:
        print(f"the `table` column of this job - case number {job['CASE_NUMBER']} -  was neither `dol_h` nor `central`")
        return False

def geocode_and_split_by_accuracy(df):
    geocode_table(df, "worksite")
    geocode_table(df, "housing")
    accurate = df.apply(lambda job: is_accurate(job), axis=1)
    accurate_jobs, inaccurate_jobs = df.copy()[accurate], df.copy()[~accurate]
    inaccurate_jobs["fixed"] = False
    print(f"There were {len(accurate_jobs)} accurate jobs.\nThere were {len(inaccurate_jobs)} inaccurate jobs. \n")
    return accurate_jobs, inaccurate_jobs

def get_column_mappings_dictionary():
        # get column mappings dataframe
        column_mappings = pd.read_excel(os.path.join(os.getcwd(), '..', 'excel_files/column_name_mappings.xlsx'))

        # get lists of column names
        mapped_old_cols = column_mappings["Scraper column name"].tolist()
        mapped_dol_cols = column_mappings["DOL column name"].tolist()

        # remove trailing white space from column names
        mapped_old_cols = [col.strip() for col in mapped_old_cols]
        mapped_dol_cols = [col.strip() for col in mapped_dol_cols]

        # get dictionary of column mappings
        column_mappings_dict = {}
        for i in range(len(mapped_old_cols)):
            column_mappings_dict[mapped_old_cols[i]] = mapped_dol_cols[i]

        return column_mappings_dict

# renames columns in df appropriately based on our excel file with column name mappings
def rename_columns(df):
    column_mappings_dict = get_column_mappings_dictionary()
    # rename columns in df using the dictionary and return the new df which results
    return df.rename(columns=column_mappings_dict)

def h2a_or_h2b(job):
    if (job["CASE_NUMBER"][2] == "3") or (job["CASE_NUMBER"][0] == "3"):
        return "H-2A"
    elif job["CASE_NUMBER"][2] == "4":
        return "H-2B"
    else:
        return ""

def get_value(job, column):
    return job[column].tolist()[0]

def get_address_columns(worksite_or_housing):
    if worksite_or_housing == "worksite":
        return worksite_address_columns
    else:
        return housing_address_columns

def handle_previously_fixed(df, i, old_job, worksite_or_housing, merge_or_update):
    # df.to_excel("before prev fixed.xlsx")
    if worksite_or_housing not in ["worksite", "housing"]:
        print("worksite_or_housing parameter to handle_previously_fixed function must be either `worksite` ot `housing`")
        return
    fixed_by = get_value(old_job, f"{worksite_or_housing}_fixed_by")
    if fixed_by in ["coordinates", "address"]:
        if fixed_by == "address":
            address_columns = get_address_columns(worksite_or_housing)
        else:
            address_columns = [f"{worksite_or_housing} coordinates", f"{worksite_or_housing} accuracy", f"{worksite_or_housing} accuracy type"]
        # for each column, assign that column's value in old_job to the i-th element in the column in df
        for column in address_columns:
            df.at[i, column] = get_value(old_job, column)
    # elif fixed_by == "impossible":
    #     pass
    # else:
    #     if merge_or_update == "update":
    #         address_columns = get_address_columns(worksite_or_housing)
    #         # print(address_columns)
    #         for column in address_columns:
    #             # print(column, ":", get_value(old_job, column))
    #             df.at[i, column] = get_value(old_job, column)
    # df.to_excel("from prev fixed.xlsx")
    return df
