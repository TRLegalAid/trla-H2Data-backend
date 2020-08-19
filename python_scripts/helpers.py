from math import isnan
import os
import pandas as pd
from geocodio import GeocodioClient
client = GeocodioClient("454565525ee5444fefef2572155e155e5248221")

def create_address_from(address, city, state, zip):
    try:
        return address + ", " + city + " " + state + " " + str(zip)
    except:
        return ""

def geocode_table(df, worksite_or_housing):

    if worksite_or_housing == "worksite":
        addresses = df.apply(lambda job: create_address_from(job["Worksite address"], job["Worksite address city"], job["Worksite address state"], job["Worksite address zip code"]), axis=1).tolist()
    elif worksite_or_housing == "housing":
        addresses = df.apply(lambda job: create_address_from(job["Housing Info/Housing Address"], job["Housing Info/City"], job["Housing Info/State"], job["Housing Info/Postal Code"]), axis=1).tolist()
    else:
        print("worksite_or_housing should be either `worksite` or `housing`")

    coordinates, accuracies, accuracy_types, failures = [], [], [], []
    failures_count, count = 0, 0
    for address in addresses:
        try:
            geocoded = client.geocode(address)
            accuracy_types.append(geocoded["results"][0]["accuracy_type"])
            coordinates.append(geocoded.coords)
            accuracies.append(geocoded.accuracy)
        except:
            coordinates.append(None)
            accuracies.append(None)
            accuracy_types.append(None)
            failures.append(address)
            failures_count += 1
        count += 1
        print(f"There have been {failures_count} failures out of {count} attempts")

    df[f"{worksite_or_housing} coordinates"] = coordinates
    df[f"{worksite_or_housing} accuracy"] = accuracies
    df[f"{worksite_or_housing} accuracy type"] = accuracy_types
    print(f"There were {failures_count} failures out of {count} attempts")

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

# function for printing dictionary
def prettier(dictionary):
    for key in dictionary:
        print(key, ": ", dictionary[key])


def check_accuracies(jobs):
    our_states = ["texas", "kentucky", "tennessee", "arkansas", "louisiana", "mississippi", "alabama"]
    accurate_jobs = []
    inaccurate_jobs = []
    bad_accuracy_types = ["place", "state", "street_center"]
    for job in jobs:
        if job["Visa type"] == "H-2A":
            # if (job["Worksite address state"].lower() in our_states) and ((job["worksite coordinates"] == None) or (job["housing coordinates"] == None) or (job["worksite accuracy"] < 0.8) or (job["housing accuracy"] < 0.8) or (job["worksite accuracy type"] in bad_accuracy_types) or (job["housing accuracy type"] in bad_accuracy_types)):
            if ((job["worksite coordinates"] == None) or (job["housing coordinates"] == None) or (job["worksite accuracy"] < 0.8) or (job["housing accuracy"] < 0.8) or (job["worksite accuracy type"] in bad_accuracy_types) or (job["housing accuracy type"] in bad_accuracy_types)):

                job["fixed"] = False
                job["worksite_fixed_by"] = "NA"
                job["housing_fixed_by"] = "NA"
                inaccurate_jobs.append(job)
            else:
                accurate_jobs.append(job)

        elif job["Visa type"] == "H-2B":
            # if (job["Worksite address state"].lower() in our_states) and ((job["worksite coordinates"] == None) or (job["worksite accuracy"] < 0.8) or (job["worksite accuracy type"] in bad_accuracy_types)):
            if (job["worksite coordinates"] == None) or (job["worksite accuracy"] < 0.8) or (job["worksite accuracy type"] in bad_accuracy_types):
                job["fixed"] = False
                job["worksite_fixed_by"] = "NA"
                job["housing_fixed_by"] = "NA"
                inaccurate_jobs.append(job)
            else:
                accurate_jobs.append(job)

        else:
            job["fixed"] = False
            job["worksite_fixed_by"] = "NA"
            job["housing_fixed_by"] = "NA"
            inaccurate_jobs.append(job)

    print(f"There were {len(accurate_jobs)} accurate jobs.")
    print(f"There were {len(inaccurate_jobs)} inaccurate jobs.")
    return accurate_jobs, inaccurate_jobs
