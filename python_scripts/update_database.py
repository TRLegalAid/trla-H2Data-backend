import requests
import helpers
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
engine = create_engine('postgres://txmzafvlwebrcr:df20d17265cf81634b9f689187248524a6fd0d56222985e2f422c71887ec6ec0@ec2-34-224-229-81.compute-1.amazonaws.com:5432/dbs39jork6o07d')
from geocodio import GeocodioClient
client = GeocodioClient("454565525ee5444fefef2572155e155e5248221")

# just for testing, REMEMBER TO REMOVE!
try:
    engine.execute("drop table low_accuracies")
except:
    pass

# sample big dataset - https://api.apify.com/v2/datasets/xe6ZzDWTPiCEB7Vw8/items?format=json&clean=1
# most recent run - https://api.apify.com/v2/acts/eytaog~apify-dol-actor-latest/runs/last/dataset/items?token=ftLRsXTA25gFTaCvcpnebavKw
latest_jobs = requests.get("https://api.apify.com/v2/acts/eytaog~apify-dol-actor-latest/runs/last/dataset/items?token=ftLRsXTA25gFTaCvcpnebavKw").json()

# parse job so it's not a nested dictionary
def parse(job):
    columns_names_dict = {"Section A": "Job Info", "Section C": "Place of Employment Info", "Section D":"Housing Info"}
    res = {}
    for key in job:
        if key not in ["Section D", "Section C", "Section A"]:
            res[key] = job[key]
        else:
            inner_dict = job[key]
            for inner_key in inner_dict:
                key_name = columns_names_dict[key] + "/" + inner_key
                res[key_name] = inner_dict[inner_key]
    return res

# add necessary columns to job
def add_necessary_columns(job):

    # create and geocode full worksite address
    worksite_full_address = helpers.create_address_from(job["Worksite address"], job["Worksite address city"], job["Worksite address state"], job["Worksite address zip code"])
    worksite_geocoded = client.geocode(worksite_full_address)
    job["worksite coordinates"] = worksite_geocoded.coords
    job["worksite accuracy"] = worksite_geocoded.accuracy
    # this might fail, but the others won't (they just return None if it's a bad address)
    try:
        job["worksite accuracy type"] = worksite_geocoded["results"][0]["accuracy_type"]
    except:
        # set to place so that it'll go in the bad zone, maybe change place to failed, but this should be fine
        job["worksite accuracy type"] = "place"

    # add source and date of run column
    job["Source"] = "Apify"
    job["Date of run"] = requests.get("https://api.apify.com/v2/acts/eytaog~apify-dol-actor-latest/runs/last?token=ftLRsXTA25gFTaCvcpnebavKw").json()["data"]["finishedAt"]

    # check if job is h2a
    if job["ETA case number"][2] == "3":
        job["Visa type"] = "H-2A"
        # create and geocode full housing address
        housing_full_address = helpers.create_address_from(job["Housing Info/Housing Address"], job["Housing Info/City"], job["Housing Info/State"], job["Housing Info/Postal Code"])
        housing_geocoded = client.geocode(housing_full_address)
        job["housing coordinates"] = housing_geocoded.coords
        job["housing accuracy"] = housing_geocoded.accuracy
        try:
            job["housing accuracy type"] = housing_geocoded["results"][0]["accuracy_type"]
        except:
            # set to place so that it'll go in the bad zone, maybe change place to failed, but this should be fine
            job["housing accuracy type"] = "place"
        # get the number of workers requested
        job["Number of Workers Requested"] = job["Job Info/Workers Needed H-2A"]

        # create W:H Ratio column
        if job["Number of Workers Requested"] > job["Housing Info/Total Occupancy"]:
            job["W to H Ratio"] = "W>H"
        elif job["Number of Workers Requested"] < job["Housing Info/Total Occupancy"]:
            job["W to H Ratio"] = "W<H"
        else:
            job["W to H Ratio"] = "W=H"

    # check if job is h2b
    elif job["ETA case number"][2] == "4":
        job["Visa type"] = "H-2B"
    else:
        job["Visa type"] = ""

    # add 0's to front of zip code if necessary
    # zip_code = job["Company zip code"]
    # job["Company zip code"] = "0" * (5 - len(zip_code)) + zip_code
    #
    # zip_code = job["Worksite address zip code"]
    # job["Worksite address zip code"] = "0" * (5 - len(zip_code)) + zip_code
    for zip_code_column_name in ["Company zip code", "Worksite address zip code",  "Place of Employment Info/Postal Code", "Housing Info/Postal Code"]:
        job[zip_code_column_name] = helpers.fix_zip_code(job.get(zip_code_column_name, None))

    return job

# parse each job then add all columns to each job
parsed_jobs = [parse(job) for job in latest_jobs]
full_jobs = [add_necessary_columns(job) for job in parsed_jobs]

accurate_jobs, inaccurate_jobs = helpers.check_accuracies(full_jobs)

# get data from postgres
all_data = pd.read_sql_query('select * from "todays_tests"', con=engine)

try:
    low_accuracies = pd.read_sql_query('select * from "low_accuracies"', con=engine)
except:
    low_accuracies = pd.DataFrame()

# loop to add each new job to df
# uncomment the line below to demonstrate/test removal of duplicates
# accurate_jobs.append({"ETA case number":"H-300-20118-520718"})
for job in accurate_jobs:
    # remove rows from all data that have duplicate case number
    all_data = all_data[all_data["ETA case number"] != job["ETA case number"]]
    try:
        # same as above but for low_accuracies, this will fail if low_accuracies is originally empty, thus the try
        low_accuracies = low_accuracies[low_accuracies["ETA case number"] != job["ETA case number"]]
    except:
        pass
    all_data = all_data.append(job, ignore_index=True)
# send updated df back to postgres
all_data.to_sql('todays_tests', engine, if_exists='replace', index=False)

# same as above but for inaccurate jobs
for job in inaccurate_jobs:
    all_data = all_data[all_data["ETA case number"] != job["ETA case number"]]
    try:
        low_accuracies = low_accuracies[low_accuracies["ETA case number"] != job["ETA case number"]]
    except:
        pass
    low_accuracies = low_accuracies.append(job, ignore_index=True)

low_accuracies.to_sql('low_accuracies', engine, if_exists='replace', index=False, dtype={"fixed": sqlalchemy.types.Boolean, "Experience Required": sqlalchemy.types.Boolean, "Multiple Worksites": sqlalchemy.types.Boolean})
