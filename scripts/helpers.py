from math import isnan
import os
import pandas as pd
from geocodio import GeocodioClient
import requests
import sqlalchemy
from colorama import Fore, Style
from inspect import getframeinfo, stack
import smtplib, ssl

def get_secret_variables():
    # LOCAL_DEV is an environment variable that I set to be "true" on my mac and "false" in the heroku config variables
    if os.getenv("LOCAL_DEV") == "true":
        import secret_variables
        return secret_variables.DATABASE_URL, secret_variables.GEOCODIO_API_KEY, secret_variables.MOST_RECENT_RUN_URL, secret_variables.DATE_OF_RUN_URL, secret_variables.ERROR_EMAIL_ADDRESS, secret_variables.ERROR_EMAIL_ADDRESS_PASSWORD
    return os.getenv("DATABASE_URL"), os.getenv("GEOCODIO_API_KEY"), os.getenv("MOST_RECENT_RUN_URL"), os.getenv("DATE_OF_RUN_URL"), os.getenv("ERROR_EMAIL_ADDRESS"), os.getenv("ERROR_EMAIL_ADDRESS_PASSWORD")
geocodio_api_key = get_secret_variables()[1]
client = GeocodioClient(geocodio_api_key)

# if running locally, don't do the state checking (to make it easier for tesing)
state_checking = not (os.getenv("LOCAL_DEV") == "true")
state_checking = True
our_states = ["texas", "tx", "kentucky", "ky", "tennessee", "tn", "arkansas", "ar", "louisiana", "la", "mississippi", "ms", "alabama", "al"]

def print_red(message):
    print(Fore.RED + message + Style.RESET_ALL)

def myprint(message, is_red="", email_also=""):
    if email_also != "yes":
        frameinfo = getframeinfo((stack()[1][0]))
        file_and_line_info = Fore.LIGHTBLUE_EX + "  (" + frameinfo.filename.split("/")[-1] + ", line " + str(frameinfo.lineno) + ")" + Style.RESET_ALL
    else:
        file_and_line_info = ""
    if is_red == "red":
        print_red(message + file_and_line_info)
    else:
        print(message + file_and_line_info)

def send_email(message):
    # if os.getenv("LOCAL_DEV") == "true":
    #     return
    email, password = get_secret_variables()[4], get_secret_variables()[5]
    port, smtp_server, context  = 465, "smtp.gmail.com", ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(email, password)
        server.sendmail(email, email, message)

def print_red_and_email(message, subject):
    frameinfo = getframeinfo((stack()[1][0]))
    file_name_and_line_info = "(" + frameinfo.filename.split("/")[-1] + ", line " + str(frameinfo.lineno) + ")"
    myprint(message + "  " + file_name_and_line_info, is_red="red", email_also="yes")
    send_email("Subject: " + subject + "\n\n" + message + "\n" + file_name_and_line_info)

bad_accuracy_types = ["place", "state", "street_center"]
column_types = {
    "fixed": sqlalchemy.types.Boolean, "Experience Required": sqlalchemy.types.Boolean, "Multiple Worksites": sqlalchemy.types.Boolean,
    "Date of run": sqlalchemy.types.DateTime, "RECEIVED_DATE": sqlalchemy.types.DateTime, "EMPLOYMENT_BEGIN_DATE": sqlalchemy.types.DateTime,
    "EMPLOYMENT_END_DATE": sqlalchemy.types.DateTime, "HOUSING_POSTAL_CODE": sqlalchemy.types.Text, "Job Info/Workers Needed Total": sqlalchemy.types.Integer,
    "PHONE_TO_APPLY": sqlalchemy.types.Text, "Place of Employment Info/Postal Code": sqlalchemy.types.Text, "TOTAL_OCCUPANCY": sqlalchemy.types.Integer,
    "TOTAL_UNITS": sqlalchemy.types.Integer, "TOTAL_WORKERS_H-2A_REQUESTED": sqlalchemy.types.Integer, "TOTAL_WORKERS_NEEDED": sqlalchemy.types.Integer,
    "WORKSITE_POSTAL_CODE": sqlalchemy.types.Text, "ATTORNEY_AGENT_PHONE": sqlalchemy.types.Text, "EMPLOYER_POC_PHONE": sqlalchemy.types.Text,
    "EMPLOYER_PHONE": sqlalchemy.types.Text, "SOC_CODE": sqlalchemy.types.Text, "NAICS_CODE": sqlalchemy.types.Text, "notes": sqlalchemy.types.Text,
    "worksite_lat": sqlalchemy.types.Float, "housing_lat": sqlalchemy.types.Float, "worksite_long": sqlalchemy.types.Float, "housing_long": sqlalchemy.types.Float,
}
housing_address_columns = ["HOUSING_ADDRESS_LOCATION", "HOUSING_CITY", "HOUSING_STATE", "HOUSING_POSTAL_CODE", "housing_lat", "housing_long", "housing accuracy", "housing accuracy type", "housing_fixed_by", "fixed"]
worksite_address_columns = ["WORKSITE_ADDRESS", "WORKSITE_CITY", "WORKSITE_STATE", "WORKSITE_POSTAL_CODE", "worksite_lat", "worksite_long", "worksite accuracy", "worksite accuracy type", "worksite_fixed_by", "fixed"]

# function for printing dictionary
def prettier(dictionary):
    for key in dictionary:
        print(key, ": ", dictionary[key])

def create_address_from(address, city, state, zip):
    try:
        return address + ", " + city + " " + state + " " + str(zip)
    except:
        return ""

def geocode_table(df, worksite_or_housing):
    myprint(f"Geocoding {worksite_or_housing}...")

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
        print_red_and_email("`worksite_or_housing` parameter in geocode_table function must be either `worksite` or `housing` or `housing addendum`", "Invalid Function Parameter")
        return

    geocoding_results = client.geocode(addresses)

    latitudes, longitudes, accuracies, accuracy_types, i = [], [], [], [], 0
    for result in geocoding_results:
        try:
            results = result['results'][0]
            accuracies.append(results['accuracy'])
            accuracy_types.append(results['accuracy_type'])
            latitudes.append(results['location']['lat'])
            longitudes.append(results['location']['lng'])
        except:
            accuracies.append(None)
            accuracy_types.append(None)
            latitudes.append(None)
            longitudes.append(None)
        i +=1

    i = len(df.columns)
    df.insert(i, f"{geocoding_type}_lat", latitudes)
    df.insert(i, f"{geocoding_type}_long", longitudes)
    df.insert(i, f"{geocoding_type} accuracy", accuracies)
    df.insert(i, f"{geocoding_type} accuracy type", accuracy_types)
    myprint(f"Finished geocoding {worksite_or_housing}.")
    return df

def geocode_and_split_by_accuracy(df, table=""):
    if table != "housing addendum":
        df = geocode_table(df, "worksite")
        df = geocode_table(df, "housing")
    else:
        geocode_table(df, "housing addendum")
    accurate = df.apply(lambda job: is_accurate(job), axis=1)
    accurate_jobs, inaccurate_jobs = df.copy()[accurate], df.copy()[~accurate]
    inaccurate_jobs["fixed"] = False
    myprint(f"There were {len(accurate_jobs)} accurate jobs.\nThere were {len(inaccurate_jobs)} inaccurate jobs.")
    return accurate_jobs, inaccurate_jobs

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
    return df

def is_accurate(job):
    if state_checking and (job["WORKSITE_STATE"].lower() not in our_states):
        return True
    if job["table"] == "central":
        if job["Visa type"] == "H-2A":
            return not ((not job["worksite accuracy type"]) or (not job["housing accuracy type"]) or (job["worksite accuracy"] < 0.8) or (job["housing accuracy"] < 0.8) or (job["worksite accuracy type"] in bad_accuracy_types) or (job["housing accuracy type"] in bad_accuracy_types))
        elif job["Visa type"] == "H-2B":
            return not ((job["worksite accuracy"] == None) or (job["worksite accuracy"] < 0.8) or (job["worksite accuracy type"] in bad_accuracy_types))
        else:
            error_message = f"The `Visa type` column of this job -case number {job['CASE_NUMBER']}- was neither `H-2A` nor `H-2B`, marking as inaccurate."
            print_red_and_email(error_message, "Bad Visa Type")
            return False

    elif job["table"] == "dol_h":
        return not ((job["housing accuracy"] == None) or (job["housing accuracy"] < 0.8) or (job["housing accuracy type"] in bad_accuracy_types))

    else:
        error_message = f"The `table` column of this job -case number {job['CASE_NUMBER']}- was neither `dol_h` nor `central`"
        print_red_and_email(error_message, "Bad Table Column Value")
        return False

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
    elif (job["CASE_NUMBER"][2] == "4") or (job["CASE_NUMBER"][0] == "4"):
        return "H-2B"
    else:
        print_red_and_email(f"Case number: {job['CASE_NUMBER']}", "Case Number Malformed")
        return ""

def get_value(job, column):
    return job[column].tolist()[0]

def get_address_columns(worksite_or_housing):
    if worksite_or_housing == "worksite":
        return worksite_address_columns
    else:
        return housing_address_columns

def remove_case_number_from_df(df, case_number):
    return df[(df["CASE_NUMBER"] != case_number) | (df["table"] == "dol_h")]

def handle_previously_fixed(df, i, old_job, worksite_or_housing, accurate_or_inaccurate=""):

    if worksite_or_housing not in ["worksite", "housing"]:
        error_message = "worksite_or_housing parameter to handle_previously_fixed function must be either `worksite` ot `housing`"
        print_red_and_email(error_message, "Invalid Function Parameter")
        return df
    if accurate_or_inaccurate == "accurate":
        for column in ["fixed", f"{worksite_or_housing}_fixed_by"]:
            df.at[i, column] = None
        return df
    fixed_by = get_value(old_job, f"{worksite_or_housing}_fixed_by")
    if fixed_by in ["coordinates", "address"]:
        if fixed_by == "address":
            address_columns = get_address_columns(worksite_or_housing)
        else:
            address_columns = [f"{worksite_or_housing}_lat", f"{worksite_or_housing}_long", f"{worksite_or_housing} accuracy", f"{worksite_or_housing} accuracy type", f"{worksite_or_housing}_fixed_by", "fixed"]
        # for each column, assign that column's value in old_job to the i-th element in the column in df
        for column in address_columns:
            df.at[i, column] = get_value(old_job, column)
    else:
        # if worksite_or_housing needs fixing in new df, mark notes column to not move to accuracies later - see lin 236
        if (not df.at[i, f"{worksite_or_housing} accuracy type"]) or (df.at[i, f"{worksite_or_housing} accuracy"] < 0.8) or (df.at[i ,f"{worksite_or_housing} accuracy type"] in bad_accuracy_types):
            df.at[i, "notes"] = "ignore (and feel free to replace) this note"

    if df.at[i, "notes"] == "ignore (and feel free to replace) this note":
        df.at[i, "fixed"] = False

    return df

def is_previously_fixed(old_job):
    return get_value(old_job, "fixed") == True

def check_and_handle_previously_fixed(data, old_job, i, accurate_or_inaccurate=""):
    if is_previously_fixed(old_job):
        myprint("PREVIOUSLY FIXED: " + get_value(old_job, "CASE_NUMBER") + " has already been fixed.")
        data = handle_previously_fixed(data, i, old_job , "worksite", accurate_or_inaccurate=accurate_or_inaccurate)
        data = handle_previously_fixed(data, i, old_job, "housing", accurate_or_inaccurate=accurate_or_inaccurate)
    return data

# if accurate_or_innacurate is "accurate" then (new_df, old_df) are the accurate new jobs and job_central (accurate old jobs), and (new_df_opposite, old_df_opposite) are the inaccurate new jobs and low_accuracies table from postgres
# if accurate_or_innacurate is "inaccurate", it's the opposite
def merge_common_rows(new_df, new_df_opposite, old_df, old_df_opposite, accurate_or_inaccurate):
    if accurate_or_inaccurate not in ["accurate", "inaccurate"]:
        error_message = "accurate_or_inaccurate parameter to merge_new_with_old_data function must be either `accurate` or `inaccurate`"
        print_red_and_email(error_message, "Invalid Function Parameter")
        return
    myprint(f"MERGING {accurate_or_inaccurate} new data...")
    old_case_numbers = old_df["CASE_NUMBER"].tolist()
    old_opposite_case_numbers = old_df_opposite["CASE_NUMBER"].tolist()
    all_old_columns = old_df.columns.tolist()
    all_new_columns = new_df.columns.tolist()
    only_old_columns = [column for column in all_old_columns if column not in all_new_columns and column != "index"]
    # add each columnd in postgres but not new to new
    for column in only_old_columns:
         new_df[column] = None
    for i, job in new_df.iterrows():
        new_case_number = job["CASE_NUMBER"]
        # if this jobs is already in postgres
        if new_case_number in old_case_numbers:
            myprint(f"DUPLICATE CASE NUMBER: {new_case_number} is in both the ({accurate_or_inaccurate}) new dataset and the {accurate_or_inaccurate} table in postgres.")
            old_job = old_df[(old_df["CASE_NUMBER"] == new_case_number) & (old_df["table"] != "dol_h")]
            # add the value of each column only found in postgres to new data
            for column in only_old_columns:
                new_df.at[i, column] = get_value(old_job, column)
            # if previously fixed, replace new address/geocoding data with that from postgres (where appropriate, depending on fixed_by columns)
            new_df = check_and_handle_previously_fixed(new_df, old_job, i, accurate_or_inaccurate=accurate_or_inaccurate)
            # remove this job from old_df, since it's in new_df and has been updated
            old_df = remove_case_number_from_df(old_df, new_case_number)
        elif new_case_number in old_opposite_case_numbers:
            # if this job is accurate in new but inaccurate in postgres
            if accurate_or_inaccurate == "accurate":
                myprint(f"DUPLICATE CASE NUMBER: {new_case_number} is in both the ({accurate_or_inaccurate}) new dataset and the low_accuracies table in postgres.")
                old_job = old_df_opposite[(old_df_opposite["CASE_NUMBER"] == new_case_number) & (old_df_opposite["table"] != "dol_h")]
                for column in only_old_columns:
                    new_df.at[i, column] = get_value(old_job, column)
                # remove it from the postgres inaccurate df (unless it comes from the additional housing table)
                old_df_opposite = remove_case_number_from_df(old_df_opposite, new_case_number)
            # if this job is inaccurate in new but accurate in postgres
            else:
                myprint(f"DUPLICATE CASE NUMBER: {new_case_number} is in both the ({accurate_or_inaccurate}) new dataset and the job_central table in postgres.")
                old_job = old_df_opposite[(old_df_opposite["CASE_NUMBER"] == new_case_number) & (old_df_opposite["table"] != "dol_h")]
                for column in only_old_columns:
                    new_df.at[i, column] = get_value(old_job, column)
                # if previously fixed, adjust it in the new data accordingly, otherwise leave it (this probably means the address has been changed since we last received it)
                if is_previously_fixed(old_job):
                    new_df = check_and_handle_previously_fixed(new_df, old_job, i)
                    if new_df.at[i, "notes"] != "ignore (and feel free to replace) this note":
                        new_df.at[i, "notes"] = "accurate"
                # remove from accurate old df
                old_df_opposite = remove_case_number_from_df(old_df_opposite, new_case_number)
        # if this jobs is only in new data, just leave it be
        else:
            pass
    # if accurate_or_inaccurate == "accurate", returns: accurate dol df, inaccurate dol df, low_accuracies table from postgres
    # if accurate_or_inaccurate == "inaccurate", returns: inaccurate dol df, accurate dol df, job_central table from postgres
    myprint(f"FINISHED merging {accurate_or_inaccurate} DOL data.")
    return new_df, new_df_opposite, old_df, old_df_opposite

def merge_all_data(accurate_new_jobs, inaccurate_new_jobs, accurate_old_jobs, inaccurate_old_jobs):
    # merge accurate and inaccurate jobs, get dataframe of postings that are only in job_central, append that to the accurate dol dataframe
    accurate_new_jobs, inaccurate_new_jobs, accurate_old_jobs, inaccurate_old_jobs = merge_common_rows(accurate_new_jobs, inaccurate_new_jobs, accurate_old_jobs, inaccurate_old_jobs, "accurate")
    inaccurate_new_jobs, accurate_new_jobs, inaccurate_old_jobs, accurate_old_jobs = merge_common_rows(inaccurate_new_jobs, accurate_new_jobs, inaccurate_old_jobs, accurate_old_jobs, "inaccurate")
    accurate_new_case_numbers = accurate_new_jobs["CASE_NUMBER"].tolist()
    only_in_accurate_old = accurate_old_jobs[~(accurate_old_jobs["CASE_NUMBER"].isin(accurate_new_case_numbers))]
    all_accurate_jobs = accurate_new_jobs.append(only_in_accurate_old, sort=True, ignore_index=True)

    # merge inaccurate jobs, remove necessary case numbers from inaccurate jobs, append jobs that are only in low_accuracies (postgres but not DOL)
    # also move any fixed inaccurates to accurates
    inaccurate_new_case_numbers = inaccurate_new_jobs["CASE_NUMBER"].tolist()
    only_in_inaccurate_old = inaccurate_old_jobs[(~(inaccurate_old_jobs["CASE_NUMBER"].isin(inaccurate_new_case_numbers))) | (inaccurate_old_jobs["table"] == "dol_h")]
    all_inaccurate_jobs = inaccurate_new_jobs.append(only_in_inaccurate_old, sort=True, ignore_index=True)
    accurates_in_inaccurates = all_inaccurate_jobs[all_inaccurate_jobs["notes"] == "accurate"]
    all_accurate_jobs = all_accurate_jobs.append(accurates_in_inaccurates, sort=True, ignore_index=True)
    all_inaccurate_jobs = all_inaccurate_jobs[all_inaccurate_jobs["notes"] != "accurate"]

    return all_accurate_jobs, all_inaccurate_jobs

def sort_df_by_date(df):
    return df.sort_values(by=["RECEIVED_DATE"], ascending=False)
