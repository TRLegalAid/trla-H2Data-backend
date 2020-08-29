import os
import helpers
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine
from geocodio import GeocodioClient
database_connection_string, geocodio_api_key = helpers.get_secret_variables()
engine, client = create_engine(database_connection_string), GeocodioClient(geocodio_api_key)

# get dol data and postgres data (accurate and inaccurate), perform necessary data management on dol data
dol_jobs = pd.read_excel(os.path.join(os.getcwd(), '..', 'excel_files/dol_data.xlsx'))
accurate_old_jobs = pd.read_sql("job_central", con=engine)
inaccurate_old_jobs = pd.read_sql("low_accuracies", con=engine)
dol_jobs = dol_jobs.drop_duplicates(subset='CASE_NUMBER', keep="last")
helpers.fix_zip_code_columns(dol_jobs, ["HOUSING_POSTAL_CODE", "EMPLOYER_POC_POSTAL_CODE", "EMPLOYER_POSTAL_CODE", "WORKSITE_POSTAL_CODE", "ATTORNEY_AGENT_POSTAL_CODE"])
dol_jobs["Source"], dol_jobs["table"] = "DOL", "central"
dol_jobs["Job Order Link"] = dol_jobs.apply(lambda job: "https://seasonaljobs.dol.gov/job-order/" + job["CASE_NUMBER"], axis=1)
def h2a_or_h2b(job):
    if job["CASE_NUMBER"][2] == "3":
        return "H-2A"
    elif job["CASE_NUMBER"][2] == "4":
        return "H-2B"
    else:
        return ""
dol_jobs['Visa type'] = dol_jobs.apply(lambda job: h2a_or_h2b(job), axis=1)
def yes_no_to_boolean(yes_no):
    if yes_no.strip() == "Y":
        return True
    elif yes_no.strip() == "N":
        return False
    else:
        print("there was an error converting the multiple worksite value to a boolean")
        return yes_no
dol_jobs["ADDENDUM_B_WORKSITE_ATTACHED"] = dol_jobs.apply(lambda job: yes_no_to_boolean(job["ADDENDUM_B_WORKSITE_ATTACHED"]), axis=1)

# geocode dol data and split by accuracy
accurate_dol_jobs, inaccurate_dol_jobs = helpers.geocode_and_split_by_accuracy(dol_jobs)

def get_value(job, column):
    return job[column].tolist()[0]

def is_previously_fixed(old_job):
    return get_value(old_job, "fixed") == True

def handle_previously_fixed(df, i, old_job, worksite_or_housing):
    if worksite_or_housing not in ["worksite", "housing"]:
        print("worksite_or_housing parameter to handle_previously_fixed function must be either `worksite` ot `housing`")
        return
    fixed_by = get_value(old_job, f"{worksite_or_housing}_fixed_by")
    if fixed_by in ["coordinates", "address"]:
        if fixed_by == "address":
            if worksite_or_housing == "worksite":
                address_columns = ["WORKSITE_ADDRESS", "WORKSITE_CITY", "WORKSITE_STATE", "WORKSITE_POSTAL_CODE", "worksite coordinates", "worksite accuracy", "worksite accuracy type"]
            elif worksite_or_housing == "housing":
                address_columns = ["HOUSING_ADDRESS_LOCATION", "HOUSING_CITY", "HOUSING_STATE", "HOUSING_POSTAL_CODE", "housing coordinates", "housing accuracy", "housing accuracy type"]
        else:
            address_columns = [f"{worksite_or_housing} coordinates", f"{worksite_or_housing} accuracy", f"{worksite_or_housing} accuracy type"]
        # for each column, assign that column's value in old_job to the i-th element in the column in df
        for column in address_columns:
            df.at[i, column] = get_value(old_job, column)
    return df

def check_and_handle_previously_fixed(data, old_job, i):
    if is_previously_fixed(old_job):
        data = handle_previously_fixed(data, i, old_job , "worksite")
        data = handle_previously_fixed(data, i, old_job, "housing")
    return data

dol_columns = dol_jobs.columns
case_numbers_to_remove_from_inaccuracies = []

# if accurate_or_innacurate is "accurate" then (dol_df, old_df) are the accurate dol jobs and job_central (from postgres), and (dol_df_opposite, old_df_opposite) are the inaccurate dol jobs and low_accuracies table from postgres
# if accurate_or_innacurate is "inaccurate", it's the opposite
def merge_dol_with_old_data(dol_df, dol_df_opposite, old_df, old_df_opposite, accurate_or_inaccurate):
    if accurate_or_inaccurate not in ["accurate", "inaccurate"]:
        print("accurate_or_inaccurate parameter to merge_dol_with_old_data function must be either `accurate` ot `inaccurate`")
        return
    old_case_numbers = old_df["CASE_NUMBER"].tolist()
    old_opposite_case_numbers = old_df_opposite["CASE_NUMBER"].tolist()
    all_old_columns = old_df.columns
    only_old_columns = [column for column in all_old_columns if column not in dol_columns and column != "index"]
    # add each columnd in postgres but not dol to dol
    for column in only_old_columns:
        dol_df[column] = None
    for i, job in dol_df.iterrows():
        dol_case_number = job["CASE_NUMBER"]
        # if this jobs is already in postgres
        if dol_case_number in old_case_numbers:
            old_job = old_df[(old_df["CASE_NUMBER"] == dol_case_number) & (old_df["table"] != "dol_h")]
            # add the value of each column only found in postgres to dol data
            for column in only_old_columns:
                dol_df.at[i, column] = get_value(old_job, column)
            # if previously ficed, replace dol address/geocoding data with that from postgres (where appropriate, depending on fixed_by columns)
            dol_df = check_and_handle_previously_fixed(dol_df, old_job, i)
        elif dol_case_number in old_opposite_case_numbers:
            # if this job is accurate in dol but inaccurate in postgres
            if accurate_or_inaccurate == "accurate":
                # just remove it from the postgres inaccurate df (unless it comes from the additional housing table)
                old_df_opposite = old_df_opposite[(old_df_opposite["CASE_NUMBER"] != dol_case_number) | (old_df_opposite["table"] == "dol_h")]
            # if this job is inaccurate in dol but accurate in postgres
            else:
                # store case number to later remove from dol inaccurates table
                case_numbers_to_remove_from_inaccuracies.append(dol_case_number)
                # get old job from job_central (old accurates) df
                old_job = old_df_opposite[old_df_opposite["CASE_NUMBER"] == dol_case_number]
                # all geocoding/address columns
                columns_not_to_change = ["WORKSITE_ADDRESS", "WORKSITE_CITY", "WORKSITE_STATE", "WORKSITE_POSTAL_CODE", "worksite coordinates", "worksite accuracy", "worksite accuracy type", "HOUSING_ADDRESS_LOCATION", "HOUSING_CITY", "HOUSING_STATE", "HOUSING_POSTAL_CODE", "housing coordinates", "housing accuracy", "housing accuracy type"]
                # get index of this job in accurate dols df - it'll be there already b/c we're merging accurate jobs before inaccurate jobs
                case_num_in_accurate_jobs_pos = np.flatnonzero([dol_df_opposite["CASE_NUMBER"] == dol_case_number])[0]
                # all non-address/geocoding columns
                columns_to_change = [columns for column in dol_columns if column not in columns_not_to_change]
                for column in columns_to_change:
                    # use the data from all dol columns except the geocoding/address ones
                    dol_df_opposite.at[case_num_in_accurate_jobs_pos, column] = job[column]
        # if this jobs is only in dol, just leave it be
        else:
            pass
    # if accurate_or_inaccurate == "accurate", returns: accurate dol df, inaccurate dol df, low_accuracies table from postgres
    # if accurate_or_inaccurate == "inaccurate", returns: inaccurate dol df, accurate dol df, job_central table from postgres
    # since those are the only ones that are modified
    return dol_df, dol_df_opposite, old_df_opposite

# merge accurate jobs, get dataframe of postings that are only in job_central, append that to the accurate dol dataframe
accurate_jobs, inaccurate_dol_jobs, inaccurate_old_jobs = merge_dol_with_old_data(accurate_dol_jobs, inaccurate_dol_jobs, accurate_old_jobs, inaccurate_old_jobs, "accurate")
accurate_case_numbers = accurate_jobs["CASE_NUMBER"].tolist()
only_in_accurate_old_jobs = accurate_old_jobs[~(accurate_old_jobs["CASE_NUMBER"].isin(accurate_case_numbers))]
accurate_jobs = accurate_jobs.append(only_in_accurate_old_jobs, sort=True, ignore_index=True)

# merge inaccurate jobs, remove necessary case numbers from inaccurate jobs, append jobs that are only in low_accuracies (postgres but not DOL)
inaccurate_jobs, accurate_jobs, accurate_old_jobs = merge_dol_with_old_data(inaccurate_dol_jobs, accurate_jobs, inaccurate_old_jobs, accurate_old_jobs, "inaccurate")
inaccurate_jobs = inaccurate_jobs[~(inaccurate_jobs["CASE_NUMBER"].isin(case_numbers_to_remove_from_inaccuracies))]
inaccurate_case_numbers = inaccurate_jobs["CASE_NUMBER"].tolist()
only_in_low_accuracies = inaccurate_old_jobs[~(inaccurate_old_jobs["CASE_NUMBER"].isin(inaccurate_case_numbers))]
inaccurate_jobs = inaccurate_jobs.append(only_in_low_accuracies, sort=True, ignore_index=True)

# push both dfs back to postgres
accurate_dol_jobs.to_sql("job_central", engine, if_exists='replace', index=False)
inaccurate_jobs.to_sql("low_accuracies", engine, if_exists='replace', index=False)
