import requests
import helpers
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from geocodio import GeocodioClient
import fake_jobs
database_connection_string, geocodio_api_key = helpers.get_secret_variables()
engine, client = create_engine(database_connection_string), GeocodioClient(geocodio_api_key)

# most recent run - https://api.apify.com/v2/acts/eytaog~apify-dol-actor-latest/runs/last/dataset/items?token=ftLRsXTA25gFTaCvcpnebavKw
# latest_jobs = requests.get("https://api.apify.com/v2/datasets/g0awKuoXUdCsY2oBa/items?format=json&clean=1").json()[:11]
latest_jobs = [fake_jobs.fake_h2a_only_housing_inaccurate_job, fake_jobs.fake_h2a_inaccurate_job]
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



def geocode(job):
    # create and geocode full worksite address
    worksite_full_address = helpers.create_address_from(job["WORKSITE_ADDRESS"], job["WORKSITE_CITY"], job["WORKSITE_STATE"], job["WORKSITE_POSTAL_CODE"])
    worksite_geocoded = client.geocode(worksite_full_address)
    job["worksite coordinates"] = worksite_geocoded.coords
    job["worksite accuracy"] = worksite_geocoded.accuracy
    job["table"] = "central"
    # this might fail, but the others won't (they just return None if it's a bad address)
    try:
        job["worksite accuracy type"] = worksite_geocoded["results"][0]["accuracy_type"]
    except:
        # set to place so that it'll go in the bad zone, maybe change place to failed, but this should be fine
        job["worksite accuracy type"] = "place"

    if job["CASE_NUMBER"][2] == "3":
        housing_full_address = helpers.create_address_from(job["HOUSING_ADDRESS_LOCATION"], job["HOUSING_CITY"], job["HOUSING_STATE"], job["WORKSITE_POSTAL_CODE"])
        housing_geocoded = client.geocode(housing_full_address)
        job["housing coordinates"] = housing_geocoded.coords
        job["housing accuracy"] = housing_geocoded.accuracy
        try:
            job["housing accuracy type"] = housing_geocoded["results"][0]["accuracy_type"]
        except:
            # set to place so that it'll go in the bad zone, maybe change place to failed, but this should be fine
            job["housing accuracy type"] = "place"
    return job

full_jobs_df = pd.DataFrame(full_jobs)
new_accurate_jobs, new_inaccurate_jobs = helpers.geocode_and_split_by_accuracy(full_jobs_df)

# get data from postgres
job_central = pd.read_sql("job_central", con=engine)
low_accuracies = pd.read_sql("low_accuracies", con=engine)

def remove_case_numbers_from_df(df, case_number):
    return df[(df["CASE_NUMBER"] != case_number) | (df["table"] == "dol_h")]

def remove_duplicates_from_postgres(new_jobs, new_opposite_jobs, accurate_or_inaccurate, job_central, low_accuracies):
    print("Removing duplicate case numbers.... \n")
    if accurate_or_inaccurate not in ["accurate", "inaccurate"]:
        print("The `accurate_or_inaccurate` parameter to the `remove_duplicates_from_postgres` function must be either `accurate` or `inaccurate`.")
        return new_jobs, job_central, low_accuracies
    # if removing duplicates using accurate new jobs
    new_jobs_case_numbers = new_jobs["CASE_NUMBER"].tolist()
    if accurate_or_inaccurate == "accurate":
        # for each case number in new jobs, remove any rows in low_accs and job_central with that case number
        for case_number in new_jobs_case_numbers:
            job_central = remove_case_numbers_from_df(job_central, case_number)
            low_accuracies = remove_case_numbers_from_df(low_accuracies, case_number)
    # if removing duplicates using inaccurate new jobs
    else:
        for case_number in new_jobs_case_numbers:
        # remove jobs with that case number from low_accuracies (unless it's a housing row)
            low_accuracies = remove_case_numbers_from_df(low_accuracies, case_number)
            # if case number is in job central, put address/geocoding info from job central and put it in new jobs, then remove that row from job central
            if case_number in job_central["CASE_NUMBER"].tolist():
                print("The case number of a new innacurate job exists in job_central. \n")
                job_in_job_central = job_central[job_central["CASE_NUMBER"] == case_number]
                job_index_in_job_central = job_in_job_central.index.tolist()[0]
                new_jobs = helpers.handle_previously_fixed(new_jobs, job_index_in_job_central, job_in_job_central, "worksite", "update")
                if helpers.get_value(job_in_job_central, "Visa type") == "H-2A":
                    new_jobs = helpers.handle_previously_fixed(new_jobs, job_index_in_job_central, job_in_job_central, "housing", "update")
                new_jobs.to_excel("ssss.xlsx")
                new_job_to_add_to_accurates = new_jobs[new_jobs["CASE_NUMBER"] == case_number]
                new_jobs = remove_case_numbers_from_df(new_jobs, case_number)
                new_opposite_jobs = new_opposite_jobs.append(new_job_to_add_to_accurates, sort=True)
                job_central = remove_case_numbers_from_df(job_central, case_number)
    print("Finished removing duplicate case numbers. \n")

    return new_jobs, new_opposite_jobs, job_central, low_accuracies




new_accurate_jobs, new_inaccurate_jobs, job_central, low_accuracies = remove_duplicates_from_postgres(new_accurate_jobs, new_inaccurate_jobs, "accurate", job_central, low_accuracies)
new_inaccurate_jobs, new_accurate_jobs, job_central, low_accuracies = remove_duplicates_from_postgres(new_inaccurate_jobs, new_accurate_jobs, "inaccurate", job_central, low_accuracies)


new_accurate_jobs.to_excel("newaccs.xlsx")
new_inaccurate_jobs.to_excel("newinaccs.xlsx")
exit()

all_accurate_jobs = job_central.append(new_accurate_jobs, sort=True)
all_inaccurate_jobs = low_accuracies.append(new_inaccurate_jobs, sort=True)

# send updated data back to postgres
all_accurate_jobs.to_sql('job_central', engine, if_exists='replace', index=False, dtype=helpers.column_types)
all_inaccurate_jobs.to_sql('low_accuracies', engine, if_exists='replace', index=False, dtype=helpers.column_types)

# geocode jobs, split by accuracy
# full_jobs_geocoded = [geocode(job) for job in full_jobs]
# accurate_jobs_list, inaccurate_jobs_list = helpers.check_accuracies(full_jobs_geocoded)


# for case_number in new_accurate_case_numbers:
#     remove_case_numbers_from_df(job_central_df, case_number)
#     remove_case_numbers_from_df(low_accuracies_df, case_number)
#
# for case_number in new_inaccurate_case_numbers:
#     remove_case_numbers_from_df(low_accuracies_df, case_number)
#     if case_number in old_accurate_case_numbers:
#         remove_case_numbers_from_df(new_inaccurate_jobs_df, case_number)



# def remove_duplicates_from_postgres(job_central_df, low_accuracies_df, jobs_list, accurate_or_inaccurate_str):
#     for job in jobs_list:
#         # remove rows from all data that have duplicate case number and haven't been fixed yet and aren't from additional housing
#         job_central_df = job_central_df[(job_central_df["CASE_NUMBER"] != job["CASE_NUMBER"]) | (job_central_df["fixed"] == True)]
#         if len(low_accuracies_df) > 0:
#             low_accuracies_df = low_accuracies_df[(low_accuracies_df["CASE_NUMBER"] != job["CASE_NUMBER"]) | (low_accuracies_df["table"] == "dol_h") | (low_accuracies_df["fixed"] == True)]
#
#         if accurate_or_inaccurate_str == "accurate":
#             if job["CASE_NUMBER"] not in job_central_df["CASE_NUMBER"].tolist():
#                 job_central_df = job_central_df.append(job, ignore_index=True)
#         elif accurate_or_inaccurate_str == "inaccurate":
#             if (len(low_accuracies_df) > 0) and (job["CASE_NUMBER"] not in low_accuracies_df["CASE_NUMBER"].tolist()):
#                 low_accuracies_df = low_accuracies_df.append(job, ignore_index=True)
#         else:
#             print("accurate_or_inaccurate_str parameter should be either accurate or inaccurate")
#
#     return job_central_df, low_accuracies_df

# job_central_df, low_accuracies_df = remove_duplicates_from_postgres(job_central_df, low_accuracies_df, accurate_jobs_list, "accurate")
# job_central_df, low_accuracies_df = remove_duplicates_from_postgres(job_central_df, low_accuracies_df, inaccurate_jobs_list, "inaccurate")








# HOUSING_POSTAL_CODE, Place of Employment Info/Postal Code
