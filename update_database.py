import requests
import pandas as pd
from sqlalchemy import create_engine
engine = create_engine('postgres://txmzafvlwebrcr:df20d17265cf81634b9f689187248524a6fd0d56222985e2f422c71887ec6ec0@ec2-34-224-229-81.compute-1.amazonaws.com:5432/dbs39jork6o07d')
from geocodio import GeocodioClient
client = GeocodioClient("454565525ee5444fefef2572155e155e5248221")

# function for printing dictionary
def prettier(dictionary):
    for key in dictionary:
        print(key, ": ", dictionary[key])

def create_address_from(address, city, state, zip):
    try:
        return address + ", " + city + " " + state + " " + str(zip)
    except:
        return "not quite"

# get latest jobs from scraper
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
    job["worksite full address"] = create_address_from(job["Worksite address"], job["Worksite address city"], job["Worksite address state"], job["Worksite address zip code"])
    worksite_geocoded = client.geocode(job["worksite full address"])
    job["worksite coordinates"] = worksite_geocoded.coords
    job["worksite accuracy"] = worksite_geocoded.accuracy

    # add source and date of run column
    job["Source"] = "Apify"
    job["Date of run"] = requests.get("https://api.apify.com/v2/acts/eytaog~apify-dol-actor-latest/runs/last?token=ftLRsXTA25gFTaCvcpnebavKw").json()["data"]["finishedAt"]

    # check if job is h2a
    if job["ETA case number"][2] == "3":
        job["Visa type"] = "H-2A"
        # create and geocode full housing address
        job["housing full address"] = create_address_from(job["Housing Info/Housing Address"], job["Housing Info/City"], job["Housing Info/State"], job["Housing Info/Postal Code"])
        housing_geocoded = client.geocode(job["housing full address"])
        job["housing coordinates"] = housing_geocoded.coords
        job["housing accuracy"] = housing_geocoded.accuracy
        # get the number of workers requested
        job["Number of Workers Requested"] = job["Job Info/Workers Needed H-2A"]

    # check if job is h2b
    elif job["ETA case number"][2] == "4":
        job["Visa type"] = "H-2B"
    else:
        job["Visa type"] = ""

    # create W:H Ratio column
    if job["Visa type"] == "H-2A":
        if job["Number of Workers Requested"] > job["Housing Info/Total Occupancy"]:
            job["W to H Ratio"] = "W>H"
        elif job["Number of Workers Requested"] < job["Housing Info/Total Occupancy"]:
            job["W to H Ratio"] = "W<H"
        else:
            job["W to H Ratio"] = "W=H"

    return job

# parse each job
parsed_jobs = [parse(job) for job in latest_jobs]

# add all columns to all jobs
full_jobs = [add_necessary_columns(job) for job in parsed_jobs]

# get data from postgres
df = pd.read_sql_query('select * from "job_postings"', con=engine)

# add each new job to df
for job in full_jobs:
    df = df.append(job, ignore_index=True)

# send updated df back to postgres
df.to_sql('job_postings', engine, if_exists='replace', index=False)
