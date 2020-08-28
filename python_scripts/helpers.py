from math import isnan
import os
import pandas as pd
from geocodio import GeocodioClient
import geocodio
import requests
import logging
logger = logging.Logger('catch_all')

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
bad_accuracy_types = ["place", "state", "street_center"]

def create_address_from(address, city, state, zip):
    try:
        return address + ", " + city + " " + state + " " + str(zip)
    except:
        return ""

def geocode_table(df, worksite_or_housing):
    print("Geocoding " + worksite_or_housing + "...")

    if worksite_or_housing == "worksite":
        addresses = df.apply(lambda job: create_address_from(job["WORKSITE_ADDRESS"], job["WORKSITE_CITY"], job["WORKSITE_STATE"], job["WORKSITE_POSTAL_CODE"]), axis=1).tolist()
    elif worksite_or_housing == "housing":
        addresses = df.apply(lambda job: create_address_from(job["HOUSING_ADDRESS_LOCATION"], job["HOUSING_CITY"], job["HOUSING_STATE"], job["HOUSING_POSTAL_CODE"]), axis=1).tolist()
    elif worksite_or_housing == "housing addendum":
        addresses = df.apply(lambda job: create_address_from(job["PHYSICAL_LOCATION_ADDRESS1"], job["PHYSICAL_LOCATION_CITY"], job["PHYSICAL_LOCATION_STATE"], job["PHYSICAL_LOCATION_POSTAL_CODE"]), axis=1).tolist()
    else:
        print("`worksite_or_housing` should be either `worksite` or `housing` or `housing addendum`")
        addresses = []

    coordinates, accuracies, accuracy_types, failures = [], [], [], []
    failures_count, count = 0, 0
    for address in addresses:
        try:
            geocoded = client.geocode(address)
            accuracy_types.append(geocoded["results"][0]["accuracy_type"])
            coordinates.append(geocoded.coords)
            accuracies.append(geocoded.accuracy)
        except Exception as e:
            print("There's been a failure, here is the error message:")
            logger.error(e, exc_info=True)
            print("\n")
            coordinates.append(None)
            accuracies.append(None)
            accuracy_types.append(None)
            failures.append(address)
            failures_count += 1
        count += 1
        if count % 20 == 0:
            print(f"There have been {failures_count} failures out of {count} attempts")
    if worksite_or_housing == "worksite":
        geocoding_type = "worksite"
    elif "housing" in worksite_or_housing:
        geocoding_type = "housing"
    else:
        geocoding_type = ""
        print("`worksite_or_housing` should be either `worksite` or `housing` or `housing addendum`")

    df[f"{geocoding_type} coordinates"] = coordinates
    df[f"{geocoding_type} accuracy"] = accuracies
    df[f"{geocoding_type} accuracy type"] = accuracy_types
    print(f"There were {failures_count} failures out of {count} attempts", "\n")


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

def check_accuracies(jobs):
    print("checking for accuracies...")
    our_states = ["texas", "kentucky", "tennessee", "arkansas", "louisiana", "mississippi", "alabama"]
    accurate_jobs = []
    inaccurate_jobs = []
    for job in jobs:
        if job["table"] == "central":
            if job["Visa type"] == "H-2A":
                # if (job["PHYSICAL_LOCATION_STATE"].lower() in our_states) and ((job["worksite coordinates"] == None) or (job["housing coordinates"] == None) or (job["worksite accuracy"] < 0.8) or (job["housing accuracy"] < 0.8) or (job["worksite accuracy type"] in bad_accuracy_types) or (job["housing accuracy type"] in bad_accuracy_types)):
                if ((job["worksite coordinates"] == None) or (job["housing coordinates"] == None) or (job["worksite accuracy"] < 0.8) or (job["housing accuracy"] < 0.8) or (job["worksite accuracy type"] in bad_accuracy_types) or (job["housing accuracy type"] in bad_accuracy_types)):

                    job["fixed"] = False
                    job["worksite_fixed_by"] = "NA"
                    job["housing_fixed_by"] = "NA"
                    inaccurate_jobs.append(job)
                else:
                    accurate_jobs.append(job)

            elif job["Visa type"] == "H-2B":
                # if (job["PHYSICAL_LOCATION_STATE"].lower() in our_states) and ((job["worksite coordinates"] == None) or (job["worksite accuracy"] < 0.8) or (job["worksite accuracy type"] in bad_accuracy_types)):
                if (job["worksite coordinates"] == None) or (job["worksite accuracy"] < 0.8) or (job["worksite accuracy type"] in bad_accuracy_types):
                    job["fixed"] = False
                    job["worksite_fixed_by"] = "NA"
                    inaccurate_jobs.append(job)
                else:
                    accurate_jobs.append(job)

            else:
                job["fixed"] = False
                job["worksite_fixed_by"] = "NA"
                job["housing_fixed_by"] = "NA"
                inaccurate_jobs.append(job)

        elif job["table"] == "dol_h":
            # if (job["PHYSICAL_LOCATION_STATE"].lower() in our_states) and ((job["housing coordinates"] == None) or (job["housing accuracy"] < 0.8) or (job["housing accuracy type"] in bad_accuracy_types)):
            if (job["housing coordinates"] == None) or (job["housing accuracy"] < 0.8) or (job["housing accuracy type"] in bad_accuracy_types):
                job["fixed"] = False
                job["housing_fixed_by"] = "NA"
                inaccurate_jobs.append(job)
            else:
                accurate_jobs.append(job)

        else:
            print(f"the `table` column of this job - case number {job['CASE_NUMBER']} -  was neither `dol_h` nor `central`")
            inaccurate_jobs.append(job)

    print(f"There were {len(accurate_jobs)} accurate jobs.")
    print(f"There were {len(inaccurate_jobs)} inaccurate jobs.")
    return accurate_jobs, inaccurate_jobs

def split_df_by_accuracies(df):
    list_of_jobs = df.to_dict(orient='records')
    accurate_jobs_list, inaccurate_jobs_list = check_accuracies(list_of_jobs)
    return pd.DataFrame(accurate_jobs_list), pd.DataFrame(inaccurate_jobs_list)

def geocode_and_split_by_accuracy(df):
    geocode_table(df, "worksite")
    geocode_table(df, "housing")
    return split_df_by_accuracies(df)

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
