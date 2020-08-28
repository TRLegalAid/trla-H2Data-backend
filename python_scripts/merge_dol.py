import os
import helpers
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine
from geocodio import GeocodioClient
database_connection_string, geocodio_api_key = helpers.get_secret_variables()
engine, client = create_engine(database_connection_string), GeocodioClient(geocodio_api_key)

# get dol data and data that's already in postgres, divide postgres data into h2a and h2b, put old data into another table
dol_jobs = pd.read_excel(os.path.join(os.getcwd(), '..', 'excel_files/dol_data.xlsx'))
dol_jobs = dol_jobs.drop_duplicates(subset='CASE_NUMBER', keep="last")
helpers.fix_zip_code_columns(dol_jobs, ["HOUSING_POSTAL_CODE", "EMPLOYER_POC_POSTAL_CODE", "EMPLOYER_POSTAL_CODE", "WORKSITE_POSTAL_CODE", "ATTORNEY_AGENT_POSTAL_CODE"])
old_jobs = pd.read_sql("job_central", con=engine)
old_h2a = old_jobs[old_jobs["Visa type"] == "H-2A"]
old_h2b = old_jobs[old_jobs["Visa type"] == "H-2B"]

# change columns names in old scraper data to match those in dol (where applicable)
old_h2a = helpers.rename_columns(old_h2a)
old_h2b = helpers.rename_columns(old_h2b)

# add necessary columns to dol data, convert "ADDENDUM_B_WORKSITE_ATTACHED" column to boolean
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

def get_value(data, column):
    return data[column].tolist()[0]

inaccurate_old_jobs = pd.read_sql("low_accuracies", con=engine)


# merge dol with scraper: if a case number exists in both tables and it hasn't been fixed before, keep all the columns, but for the
# columns with the same names, use the value in dol, and change the "Source" value for that row to "DOL". If it has been fixed
# before, keep the geocoding and address data that was already in postgres but replace everything else with the DOL data.
# If a case number only exits in one table, just add the row as is
dol_columns = dol_jobs.columns
accurate_scraper_columns = old_h2a.columns
cols_only_in_central = [column for column in accurate_scraper_columns if column not in dol_columns and column != "index"]
accurate_old_h2a_case_numbers = old_h2a["CASE_NUMBER"].tolist()
inaccurate_old_case_numbers = inaccurate_old_jobs["CASE_NUMBER"].tolist()

for column in cols_only_in_central:
    accurate_dol_jobs[column] = None

for i, job in accurate_dol_jobs.iterrows():
    dol_case_number = job["CASE_NUMBER"]
    # check if case number exists in job_central - if it does, merge appropriately
    if dol_case_number in accurate_old_h2a_case_numbers:
        job_in_old_data = old_h2a[old_h2a["CASE_NUMBER"] == dol_case_number]
        # add the columns that are in the old_h2a but not in dol_jobs to this row
        for column in cols_only_in_scraper:
            accurate_dol_jobs.at[i, column] = get_value(job_in_old_data, column)

        job_previously_fixed = get_value(job_in_old_data, "fixed") == True
        # if a job has already been fixed, replace the dol address/geocoding data with the old (scraper) address/geocoding data
        if job_previously_fixed:
            worksite_fixed_by = get_value(job_in_old_data, "worksite_fixed_by")
            if worksite_fixed_by in ["coordinates", "address"]:
                if worksite_fixed_by == "address":
                    worksite_address_columns = ["WORKSITE_ADDRESS", "WORKSITE_CITY", "WORKSITE_STATE", "WORKSITE_POSTAL_CODE", "worksite coordinates", "worksite accuracy", "worksite accuracy type"]
                else:
                    worksite_address_columns = ["worksite coordinates", "worksite accuracy", "worksite accuracy type"]
                for column in worksite_address_columns:
                    accurate_dol_jobs.at[i, column] = get_value(job_in_old_data, column)

            housing_fixed_by = get_value(job_in_old_data, "housing_fixed_by")
            if housing_fixed_by in ["coordinates", "address"]:
                if housing_fixed_by == "address":
                    housing_address_columns = ["HOUSING_ADDRESS_LOCATION", "HOUSING_CITY", "HOUSING_STATE", "HOUSING_POSTAL_CODE", "housing coordinates", "housing accuracy", "housing accuracy type"]
                else:
                    housing_address_columns = ["housing coordinates", "housing accuracy", "housing accuracy type"]
                for column in housing_address_columns:
                    accurate_dol_jobs.at[i, column] = get_value(job_in_old_data, column)
    # check if case number exists in low_accuracies table, if so remove it
    elif dol_case_number in inaccurate_old_case_numbers:
        inaccurate_old_case_numbers = inaccurate_old_case_numbers[inaccurate_old_case_numbers["CASE_NUMBER"] != dol_case_number]

    # if the case number is only in the dol data we don't have to do anything to it
    else:
        pass

# get dataframe of postings that are only in job_central and append that to the accurate dol dataframe
only_in_h2a = old_h2a[old_h2a.apply(lambda job: job["CASE_NUMBER"] not in accurate_dol_jobs["CASE_NUMBER"].tolist(), axis=1)]
accurate_dol_jobs = accurate_dol_jobs.append(only_in_h2a, sort=True, ignore_index=True)
# append job_central h2bs to dataframe
accurate_dol_jobs = accurate_dol_jobs.append(old_h2b, sort=True, ignore_index=True)
# push merged accurate data back up to job_central
accurate_dol_jobs.to_sql("job_central", engine, if_exists='replace', index=False)

# now merge the inaccurate dol data with the scraper data
inaccurate_scraper_columns = inaccurate_old_jobs.columns
cols_only_in_low_acc = [column for column in inaccurate_scraper_columns if column not in dol_columns and column != "index"]
for column in cols_only_in_low_acc:
    inaccurate_dol_jobs[column] = None
case_numbers_to_remove = []
for i, job in inaccurate_dol_jobs.iterrows():
    dol_case_number = job["CASE_NUMBER"]
    if dol_case_number in inaccurate_old_case_numbers:
        job_in_old_data = inaccurate_old_jobs[(inaccurate_old_jobs["CASE_NUMBER"] == dol_case_number) & (inaccurate_old_jobs["table"] != "dol_h")]
        for column in cols_only_in_scraper:
            inaccurate_dol_jobs.at[i, column] = get_value(job_in_old_data, column)
        job_previously_fixed = get_value(job_in_old_data, "fixed") == True
        if job_previously_fixed:
            worksite_fixed_by = get_value(job_in_old_data, "worksite_fixed_by")
            if worksite_fixed_by in ["coordinates", "address"]:
                if worksite_fixed_by == "address":
                    worksite_address_columns = ["WORKSITE_ADDRESS", "WORKSITE_CITY", "WORKSITE_STATE", "WORKSITE_POSTAL_CODE", "worksite coordinates", "worksite accuracy", "worksite accuracy type"]
                else:
                    worksite_address_columns = ["worksite coordinates", "worksite accuracy", "worksite accuracy type"]
                for column in worksite_address_columns:
                    inaccurate_dol_jobs.at[i, column] = get_value(job_in_old_data, column)

            housing_fixed_by = get_value(job_in_old_data, "housing_fixed_by")
            if housing_fixed_by in ["coordinates", "address"]:
                if housing_fixed_by == "address":
                    housing_address_columns = ["HOUSING_ADDRESS_LOCATION", "HOUSING_CITY", "HOUSING_STATE", "HOUSING_POSTAL_CODE", "housing coordinates", "housing accuracy", "housing accuracy type"]
                else:
                    housing_address_columns = ["housing coordinates", "housing accuracy", "housing accuracy type"]
                for column in housing_address_columns:
                    inaccurate_dol_jobs.at[i, column] = get_value(job_in_old_data, column)

    # check if case number in accurate job_central case numbers, if so update it in job_central and don't put it in low_accuracies
    elif dol_case_number in accurate_old_h2a_case_numbers:
        case_numbers_to_remove.append(dol_case_number)
        job_in_old_data = old_h2a[old_h2a["CASE_NUMBER"] == dol_case_number]
        columns_not_to_change = ["WORKSITE_ADDRESS", "WORKSITE_CITY", "WORKSITE_STATE", "WORKSITE_POSTAL_CODE", "worksite coordinates", "worksite accuracy", "worksite accuracy type", "HOUSING_ADDRESS_LOCATION", "HOUSING_CITY", "HOUSING_STATE", "HOUSING_POSTAL_CODE", "housing coordinates", "housing accuracy", "housing accuracy type"]
        case_num_in_accurate_jobs_pos = np.flatnonzero([accurate_dol_jobs["CASE_NUMBER"] == dol_case_number])[0]
        columns_to_change = [columns for column in dol_columns if column not in columns_not_to_change]
        for column in columns_to_change:
            accurate_dol_jobs.at[case_num_in_accurate_jobs_pos, column] = job[column]

    # if the case number is only in the dol data we don't have to do anything to it
    else:
        pass

inaccurate_jobs = inaccurate_dol_jobs[inaccurate_dol_jobs["CASE_NUMBER"] not in case_numbers_to_remove]
only_in_low_accuracies = inaccurate_old_jobs[inaccurate_old_jobs["CASE_NUMBER"] not in inaccurate_jobs["CASE_NUMBER"].tolist()]
inaccurate_jobs = inaccurate_jobs.append(only_in_low_accuracies, sort=True, ignore_index=True)
inaccurate_jobs.to_sql("low_accuracies", engine, if_exists='replace', index=False)
