import os
import helpers
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from geocodio import GeocodioClient
database_connection_string, geocodio_api_key = helpers.get_secret_variables()
engine, client = create_engine(database_connection_string), GeocodioClient(geocodio_api_key)

# get dol data and data that's already in postgres, divide postgres data into h2a and h2b, put old data into another table
dol_jobs = pd.read_excel(os.path.join(os.getcwd(), '..', 'excel_files/dol_data.xlsx'))
helpers.fix_zip_code_columns(dol_jobs, ["HOUSING_POSTAL_CODE", "EMPLOYER_POC_POSTAL_CODE", "EMPLOYER_POSTAL_CODE", "WORKSITE_POSTAL_CODE", "ATTORNEY_AGENT_POSTAL_CODE"])
old_jobs = pd.read_sql("job_central", con=engine)
todays_date = datetime.today().strftime('%Y-%m-%d')
old_jobs.to_sql(f"archive_{todays_date}", engine, if_exists='replace', index=False)

old_h2a = old_jobs[old_jobs["Visa type"] == "H-2A"]
old_h2b = old_jobs[old_jobs["Visa type"] == "H-2B"]

# change columns names in old scraper data to match those in dol (where applicable)
old_h2a = helpers.rename_columns(old_h2a)
old_h2b = helpers.rename_columns(old_h2b)

# add necessary columns to dol data, convert "Multiple Worksites" column to boolean
dol_jobs["Source"], dol_jobs["table"] = "DOL", "housing"
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
dol_jobs["Multiple Worksites"] = dol_jobs.apply(lambda job: yes_no_to_boolean(job["Multiple Worksites"]), axis=1)

# geocode dol data and split by accuracy
accurate_dol_jobs, inaccurate_dol_jobs = helpers.geocode_and_split_by_accuracy(dol_jobs)

def get_value(data, column):
    return data[column].tolist()[0]

# merge dol with scraper: if a case number exists in both tables, keep all the columns, but for the columns with the same names, use the value in dol, and change the "Source" value for that row to "DOL". if a case number only exits in one table, just add the row as is
dol_columns, scraper_columns = dol_jobs.columns.tolist(), old_h2a.columns.tolist()
cols_only_in_scraper = [column for column in scraper_columns if column not in dol_columns and column != "index"]
for column in cols_only_in_scraper:
    accurate_dol_jobs[column], inaccurate_dol_jobs[column] = None, None
scraper_h2a_case_numbers = old_h2a["CASE_NUMBER"].tolist()
for i, job in accurate_dol_jobs.iterrows():
    dol_case_number = job["CASE_NUMBER"]
    if dol_case_number in scraper_h2a_case_numbers:
        job_in_scraper_data = old_h2a[old_h2a["CASE_NUMBER"] == dol_case_number]
        # add the columns that are in the old_h2a but not in dol_jobs to this row
        for column in cols_only_in_scraper:
            accurate_dol_jobs.at[i, column] = get_value(job_in_scraper_data, column)

        job_previously_fixed = inaccurate_dol_jobs.at[i, "fixed"] == True
        # job_previously_fixed = get_value(job_in_scraper_data, "fixed") == True
        # if a job has already been fixed, replace the dol address/geocoding data with the old (scraper) address/geocoding data
        if job_previously_fixed:
            worksite_fixed_by = get_value(job_in_scraper_data, "worksite_fixed_by")
            if worksite_fixed_by in ["coordinates", "address"]:
                if worksite_fixed_by == "address":
                    worksite_address_columns = ["WORKSITE_ADDRESS", "WORKSITE_CITY", "WORKSITE_STATE", "WORKSITE_POSTAL_CODE", "worksite coordinates", "worksite accuracy", "worksite accuracy type"]
                else:
                    worksite_address_columns = ["worksite coordinates", "worksite accuracy", "worksite accuracy type"]
                for column in worksite_address_columns:
                    accurate_dol_jobs.at[i, column] = get_value(job_in_scraper_data, column)

            housing_fixed_by = job_in_scraper_data["housing_fixed_by"].tolist()[0]
            if housing_fixed_by in ["coordinates", "address"]:
                if housing_fixed_by == "address":
                    housing_address_columns = ["HOUSING_ADDRESS_LOCATION", "HOUSING_CITY", "HOUSING_STATE", "HOUSING_POSTAL_CODE", "housing coordinates", "housing accuracy", "housing accuracy type"]
                else:
                    housing_address_columns = ["housing coordinates", "housing accuracy", "housing accuracy type"]
                for column in housing_address_columns:
                    accurate_dol_jobs.at[i, column] = get_value(job_in_scraper_data, column)

# now merge the inaccurate dol data with the scraper data
for i, job in inaccurate_dol_jobs.iterrows():
    dol_case_number = job["CASE_NUMBER"]
    if dol_case_number in scraper_h2a_case_numbers:
        job_in_scraper_data = old_h2a[old_h2a["CASE_NUMBER"] == dol_case_number]
        # add the columns that are in the old_h2a but not in dol_jobs to this row
        for column in cols_only_in_scraper:
            inaccurate_dol_jobs.at[i, column] = get_value(job_in_scraper_data, column)

        job_previously_fixed = inaccurate_dol_jobs.at[i, "fixed"] == True
        # job_previously_fixed = get_value(job_in_scraper_data, "fixed") == True
        # if a job has already been fixed, replace the dol address/geocoding data with the old (scraper) address/geocoding data
        if job_previously_fixed:
            worksite_fixed_by = get_value(job_in_scraper_data, "worksite_fixed_by")
            if worksite_fixed_by in ["coordinates", "address"]:
                if worksite_fixed_by == "address":
                    worksite_address_columns = ["WORKSITE_ADDRESS", "WORKSITE_CITY", "WORKSITE_STATE", "WORKSITE_POSTAL_CODE", "worksite coordinates", "worksite accuracy", "worksite accuracy type"]
                else:
                    worksite_address_columns = ["worksite coordinates", "worksite accuracy", "worksite accuracy type"]
                for column in worksite_address_columns:
                    inaccurate_dol_jobs.at[i, column] = get_value(job_in_scraper_data, column)

            housing_fixed_by = job_in_scraper_data["housing_fixed_by"].tolist()[0]
            if housing_fixed_by in ["coordinates", "address"]:
                if housing_fixed_by == "address":
                    housing_address_columns = ["HOUSING_ADDRESS_LOCATION", "HOUSING_CITY", "HOUSING_STATE", "HOUSING_POSTAL_CODE", "housing coordinates", "housing accuracy", "housing accuracy type"]
                else:
                    housing_address_columns = ["housing coordinates", "housing accuracy", "housing accuracy type"]
                for column in housing_address_columns:
                    inaccurate_dol_jobs.at[i, column] = get_value(job_in_scraper_data, column)

# separate inaccurate dol data already fixed and not already fixed, add the already fixed ones to the accurate data
innacurate_dol_fixed = innacurate_dol_jobs[innacurate_dol_jobs["fixed"] == True]
accurate_dol_jobs = accurate_dol_jobs.append(innacurate_dol_fixed, sort=True, ignore_index=True)
innacurate_dol_not_fixed = innacurate_dol_jobs[innacurate_dol_jobs["fixed"] == False]

# get dataframe of postings that are only in the scraper data and append that to the dol dataframe
only_in_h2a = old_h2a[old_h2a.apply(lambda job: job["CASE_NUMBER"] not in dol_jobs["CASE_NUMBER"].tolist(), axis=1)]
accurate_dol_jobs = accurate_dol_jobs.append(only_in_h2a, sort=True, ignore_index=True)

# append h2b to dataframe
accurate_dol_jobs = accurate_dol_jobs.append(old_h2b, sort=True, ignore_index=True)

# push merged data back up to postgres
accurate_dol_jobs.to_sql("job_central", engine, if_exists='replace', index=False)
innacurate_dol_not_fixed.to_sql("low_accuracies", engine, if_exists='append', index=False)
