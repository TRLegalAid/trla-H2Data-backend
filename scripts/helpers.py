"""This file contains various functions that are used thorughout the application."""

from math import isnan
import os
import pandas as pd
from geocodio import GeocodioClient
import requests
import sqlalchemy
from colorama import Fore, Style
from inspect import getframeinfo, stack
import smtplib, ssl
from datetime import datetime
from pytz import timezone
from sqlalchemy import create_engine
from dotenv import load_dotenv
load_dotenv()

geocodio_api_key = os.getenv("GEOCODIO_API_KEY")
client = GeocodioClient(geocodio_api_key)

# when this is True we will only check for accuracy jobs located in `our_states`
state_checking = True
our_states = ["texas", "tx", "kentucky", "ky", "tennessee", "tn", "arkansas", "ar", "louisiana", "la", "mississippi", "ms", "alabama", "al"]

# prints `message` in red
def print_red(message):
    print(Fore.RED + message + Style.RESET_ALL)

# prints `message` along with the file and line info where from which the print function was called
# if is_red=="red", the message will be printed in red
# if emai_also=="yes", an email will be sent as well
def myprint(message, is_red="", email_also=""):
    try:
        if email_also != "yes":
            frameinfo = getframeinfo((stack()[1][0]))
            file_and_line_info = Fore.LIGHTBLUE_EX + "  (" + frameinfo.filename.split("/")[-1] + ", line " + str(frameinfo.lineno) + ")" + Style.RESET_ALL
        else:
            file_and_line_info = ""
        if is_red == "red":
            print_red(message + file_and_line_info)
        else:
            print(message + file_and_line_info)
    except:
        print(message + "(there was an error with `myprint`)")

# sends `message` to the email account speciifed in the environment variables, unless the local_dev env variable is true
def send_email(message):
    if os.getenv("LOCAL_DEV") == "true":
        return
    email, password = os.getenv("ERROR_EMAIL_ADDRESS"), os.getenv("ERROR_EMAIL_ADDRESS_PASSWORD")
    port, smtp_server, context  = 465, "smtp.gmail.com", ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(email, password)
        server.sendmail(email, email, message)

# prints `message` in red and sends email with subject `subject` and body `message`
def print_red_and_email(message, subject):
    frameinfo = getframeinfo((stack()[1][0]))
    file_name_and_line_info = "(" + frameinfo.filename.split("/")[-1] + ", line " + str(frameinfo.lineno) + ")"
    myprint(message + "  " + file_name_and_line_info, is_red="red", email_also="yes")
    send_email("Subject: " + subject + "\n\n" + message + "\n" + file_name_and_line_info)

# return an engine for the local postgres database if local_dev, else for the heroku-hosted database
# if force_cloud is True it return the engine for the heroku database regardless of local_dev
def get_database_engine(force_cloud=False):
    if force_cloud or (not (os.getenv("LOCAL_DEV") == "true")):
        return create_engine(os.getenv("DATABASE_URL_NEW"))
    else:
        return create_engine(os.getenv("LOCAL_DATABASE_URL"))

# asks user whether they want database engine to be for local database or heroku database
# if running a real task locally, respond yes
if os.getenv("LOCAL_DEV") == "true":
    force_cloud_input = input("Run on real database? If doing this, be careful! Enter y for yes, n for no. ").lower().strip()
    force_cloud = force_cloud_input in ["y", "yes"]
    if force_cloud:
        myprint("Ok, running on real database.")
    else:
        myprint("Ok, running on local database.")
else:
    force_cloud = False

engine = get_database_engine(force_cloud=force_cloud)

# geocoding accuracy types that will be marked as inaccurate
bad_accuracy_types = ["place", "state", "street_center"]

# column type specifications to be used as `dtype` parameter of pandas.DataFrame.to_sql() if replacing a database table or creating a new one
column_types = {
    "fixed": sqlalchemy.types.Boolean, "Experience Required": sqlalchemy.types.Boolean, "Multiple Worksites": sqlalchemy.types.Boolean,
    "Date of run": sqlalchemy.types.DateTime, "RECEIVED_DATE": sqlalchemy.types.DateTime, "EMPLOYMENT_BEGIN_DATE": sqlalchemy.types.DateTime,
    "EMPLOYMENT_END_DATE": sqlalchemy.types.DateTime, "HOUSING_POSTAL_CODE": sqlalchemy.types.Text, "Job Info/Workers Needed Total": sqlalchemy.types.Integer,
    "PHONE_TO_APPLY": sqlalchemy.types.Text, "Place of Employment Info/Postal Code": sqlalchemy.types.Text, "TOTAL_OCCUPANCY": sqlalchemy.types.Integer,
    "TOTAL_UNITS": sqlalchemy.types.Integer, "TOTAL_WORKERS_H-2A_REQUESTED": sqlalchemy.types.Integer, "TOTAL_WORKERS_NEEDED": sqlalchemy.types.Integer,
    "WORKSITE_POSTAL_CODE": sqlalchemy.types.Text, "ATTORNEY_AGENT_PHONE": sqlalchemy.types.Text, "EMPLOYER_POC_PHONE": sqlalchemy.types.Text,
    "EMPLOYER_PHONE": sqlalchemy.types.Text, "SOC_CODE": sqlalchemy.types.Text, "NAICS_CODE": sqlalchemy.types.Text, "notes": sqlalchemy.types.Text,
    "worksite_lat": sqlalchemy.types.Float, "housing_lat": sqlalchemy.types.Float, "worksite_long": sqlalchemy.types.Float, "housing_long": sqlalchemy.types.Float,
    "790A_ADDENDUM_B_ATTACHED": sqlalchemy.types.Boolean, "OTHER_WORKSITE_LOCATION": sqlalchemy.types.Boolean, "SUPERVISE_OTHER_EMP": sqlalchemy.types.Boolean,
    "790A_addendum_a_attached": sqlalchemy.types.Boolean,"ADDENDUM_B_HOUSING_ATTACHED": sqlalchemy.types.Boolean, "APPENDIX_A_ATTACHED": sqlalchemy.types.Boolean, "CRIMINAL_BACKGROUND_CHECK": sqlalchemy.types.Boolean,
    "DRIVER_REQUIREMENTS": sqlalchemy.types.Boolean, "DRUG_SCREEN": sqlalchemy.types.Boolean, "EMERGENCY_FILING": sqlalchemy.types.Boolean, "EXTENSIVE_SITTING_WALKING": sqlalchemy.types.Boolean,
    "EXTENSIVE_PUSHING_PULLING": sqlalchemy.types.Boolean, "EXPOSURE_TO_TEMPERATURES": sqlalchemy.types.Boolean, "FREQUENT_STOOPING_BENDING_OVER": sqlalchemy.types.Boolean,
    "H-2A_LABOR_CONTRACTOR": sqlalchemy.types.Boolean, "HOUSING_COMPLIANCE_FEDERAL": sqlalchemy.types.Boolean, "HOUSING_COMPLIANCE_STATE": sqlalchemy.types.Boolean, "HOUSING_COMPLIANCE_LOCAL": sqlalchemy.types.Boolean,
    "HOUSING_TRANSPORTATION": sqlalchemy.types.Boolean, "JOINT_EMPLOYER_APPENDIX_A_ATTACHED": sqlalchemy.types.Boolean, "LIFTING_REQUIREMENTS": sqlalchemy.types.Boolean, "MEALS_PROVIDED": sqlalchemy.types.Boolean,
    "ON_CALL_REQUIREMENT": sqlalchemy.types.Boolean, "REPETITIVE_MOVEMENTS": sqlalchemy.types.Boolean, "SURETY_BOND_ATTACHED": sqlalchemy.types.Boolean, "WORK_CONTRACTS_ATTACHED": sqlalchemy.types.Boolean,
    "CERTIFICATION_REQUIREMENTS": sqlalchemy.types.Boolean, "DECISION_DATE": sqlalchemy.types.DateTime
    # "WORKSITE_ADDRESS": sqlalchemy.types.Text, "WORKSITE_CITY": sqalchemy.types.Text
}

# columns to do with housing / worksite addresses
housing_address_columns = ["HOUSING_ADDRESS_LOCATION", "HOUSING_CITY", "HOUSING_STATE", "HOUSING_POSTAL_CODE", "housing_lat", "housing_long", "housing accuracy", "housing accuracy type", "housing_fixed_by", "fixed"]
worksite_address_columns = ["WORKSITE_ADDRESS", "WORKSITE_CITY", "WORKSITE_STATE", "WORKSITE_POSTAL_CODE", "worksite_lat", "worksite_long", "worksite accuracy", "worksite accuracy type", "worksite_fixed_by", "fixed"]

# h2a jobs that we've found without housing info - here because we email ourselves when we find one that hasn't been found yet (when we find one we add it to this list so we don't get emailed about it again and again)
h2as_without_housing = ["H-300-20306-894174", "H-300-20293-882133", "H-300-20288-878041", "H-300-20314-904174",
                        "H-300-20321-913675", "H-300-20317-909670", "H-300-20325-921120", "H-300-20338-936688",
                        "H-300-20349-957303", "H-300-20344-944656"]

# function for printing dictionary with each key, value pair on its own line
def prettier(dictionary):
    for key in dictionary:
        print(key, ": ", dictionary[key])

def convert_date_to_string(datetime):
    if pd.isna(datetime):
        return ""
    return datetime.strftime("%m/%d/%Y, %H:%M:%S")

# returns empty string is `object` is null, otherwise just `object`
def handle_null(object):
    if pd.isnull(object):
        return ""
    else:
        return str(object)

# returns a list of lists containing all the elements in a_list where each list's lenght is no more than max_items_per_part
def split_into_parts(a_list, max_items_per_part):

    if len(a_list) < max_items_per_part:
        return [a_list]
    else:
        num_parts = len(a_list) // max_items_per_part

        res = []
        start = 0
        end = max_items_per_part

        for num in range(num_parts):
            res.append(a_list[start:end])

            if num != num_parts - 1:
                start += max_items_per_part
                end += max_items_per_part

        res.append(a_list[end:])

        return res

def create_address_from(address, city, state, zip):
    return handle_null(address) + ", " + handle_null(city) + " " + handle_null(state) + " " + handle_null(str(zip))

# geocodes a DataFrame `df`, adding columns fo0r latitude, longitude, accuracy, and accuracy type
# worksite_or_housing is either "worksite" or "housing" and specifies whether to geocode housing or worksite columns
# if `check_previously_geocoded, uses the materialized postgres view `previously_geocoded` to geocode addresses that
# we've already geocoded without using Geocodio so as to save credits
# return the DataFrame with geocoding results columns added
def geocode_table(df, worksite_or_housing, check_previously_geocoded=False):
    myprint(f"Geocoding {worksite_or_housing}...")

    if check_previously_geocoded:
        make_query("REFRESH MATERIALIZED VIEW previously_geocoded")

        if worksite_or_housing == "worksite":
            df["address_id"] = df.apply(lambda job: (handle_null(job["WORKSITE_ADDRESS"]) + handle_null(job["WORKSITE_CITY"]) +
                                                     handle_null(job["WORKSITE_STATE"]) + handle_null(job["WORKSITE_POSTAL_CODE"])).lower(), axis=1)
        else:
            df["address_id"] = df.apply(lambda job: (handle_null(job["HOUSING_ADDRESS_LOCATION"]) + handle_null(job["HOUSING_CITY"]) +
                                                     handle_null(job["HOUSING_STATE"]) + handle_null(job["HOUSING_POSTAL_CODE"])).lower(), axis=1)
        df["previously_geocoded"] = False
        df[f"{worksite_or_housing}_lat"] = None
        df[f"{worksite_or_housing}_long"] = None
        df[f"{worksite_or_housing} accuracy"] = None
        df[f"{worksite_or_housing} accuracy type"] = ""

        errors = 0
        for i, job in df.iterrows():
            # won't work if the full address has certain special characters. should probably fix this but it's rather rare
            try:
                previous_geocode = pd.read_sql(f"""SELECT * FROM previously_geocoded WHERE full_address = '{job["address_id"]}'""", con=engine)
            except:
                previous_geocode = pd.DataFrame()
                myprint(f"""Failed to query previously_geocoded for address '{job["address_id"]}', the {i + 1}th row.""")
                errors += 1
            if not previous_geocode.empty:
                myprint(f"""'{job["address_id"]}' - the {i + 1}th row - is previously geocoded.""")
                assert len(previous_geocode) == 1
                df.at[i, f"{worksite_or_housing}_lat"] = get_value(previous_geocode, "latitude")
                df.at[i, f"{worksite_or_housing}_long"] = get_value(previous_geocode, "longitude")
                df.at[i, f"{worksite_or_housing} accuracy"] = get_value(previous_geocode, "accuracy")
                df.at[i, f"{worksite_or_housing} accuracy type"] = get_value(previous_geocode, "accuracy_type")
                df.at[i, "previously_geocoded"] = True

        myprint(f"There were {errors} errors checking for previous geocoding.")
        df = df.drop(columns=["address_id"])

        previously_geocoded = df[df["previously_geocoded"]]
        df = df[~(df["previously_geocoded"])]

        myprint(f"{len(previously_geocoded)} rows have already been geocoded and {len(df)} rows still need to be geocoded.")

        df = df.drop(columns=["previously_geocoded"])
        previously_geocoded.drop(columns=["previously_geocoded"], inplace=True)

    if not df.empty:

        if worksite_or_housing == "worksite":
            addresses = df.apply(lambda job: create_address_from(job["WORKSITE_ADDRESS"], job["WORKSITE_CITY"], job["WORKSITE_STATE"], job["WORKSITE_POSTAL_CODE"]), axis=1).tolist()
        elif worksite_or_housing == "housing":
            addresses = df.apply(lambda job: create_address_from(job["HOUSING_ADDRESS_LOCATION"], job["HOUSING_CITY"], job["HOUSING_STATE"], job["HOUSING_POSTAL_CODE"]), axis=1).tolist()
        else:
            print_red_and_email("`worksite_or_housing` parameter in geocode_table function must be either `worksite` or `housing` or `housing addendum`", "Invalid Function Parameter")
            return

        # handles case of more than 10000 addresses - because geocodio api won't batch geocode with more than 10000 addresses at once
        addresses_split = split_into_parts(addresses, 9999)
        geocoding_results = []
        for these_addresses in addresses_split:
            geocoding_results += client.geocode(these_addresses)
        assert len(geocoding_results) == len(addresses)


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
        df[f"{worksite_or_housing}_lat"] = latitudes
        df[f"{worksite_or_housing}_long"] = longitudes
        df[f"{worksite_or_housing} accuracy"] = accuracies
        df[f"{worksite_or_housing} accuracy type"] = accuracy_types
        myprint(f"Finished geocoding {worksite_or_housing}.")

    if check_previously_geocoded:
        df = df.append(previously_geocoded)

    # # uncomment to save excel file with geocoding results
    # # NOTE: that when running this chunk on a Windows machine, I got a ValueError: Invalid format string
    # # when trying to assign now. There may be platform-specific differences to that strftime format: https://strftime.org/
    # now = datetime.now(tz=timezone('US/Eastern')).strftime("%I.%M%.%S_%p_%B_%d_%Y")
    # df.to_excel(f"dol_data/geocoded_{worksite_or_housing}.xlsx")
    # myprint("Backed up geocoding results")

    return df

# geocodes `df` and returns two dataframes - one with accuratly geocoded rows and one with inaccurate rows
def geocode_and_split_by_accuracy(df, table=""):
    if table == "dol_h2b":
        df = geocode_table(df, "worksite", check_previously_geocoded=True)
    elif table == "housing addendum":
        df = geocode_table(df, "housing", check_previously_geocoded=True)
    elif table == "dol_h2a":
        df = geocode_table(df, "worksite", check_previously_geocoded=True)
        df = geocode_table(df, "housing", check_previously_geocoded=True)
    else:
        df = geocode_table(df, "worksite")
        if "HOUSING_ADDRESS_LOCATION" in df.columns:
            df = geocode_table(df, "housing")
        else:
            print_red_and_email("Not geocoding housing because HOUSING_ADDRESS_LOCATION is not present. This should be fine, and hopefully just means there were only H-2B jobs in today's run, but you may want to check.", "Not geocoding housing today")

    housing_addendum = (table == "housing addendum")
    accurate = df.apply(lambda job: is_accurate(job, housing_addendum=housing_addendum), axis=1)
    accurate_jobs, inaccurate_jobs = df.copy()[accurate], df.copy()[~accurate]
    inaccurate_jobs["fixed"] = False

    myprint(f"There were {len(accurate_jobs)} accurate jobs.\nThere were {len(inaccurate_jobs)} inaccurate jobs.")

    return accurate_jobs, inaccurate_jobs

# appends zeros to front of zip_code so it has length 5
def fix_zip_code(zip_code):
    if isinstance(zip_code, str):
        return ("0" * (5 - len(zip_code))) + zip_code
    elif zip_code == None or isnan(zip_code):
        return ""
    else:
        zip_code = str(int(zip_code))
        return ("0" * (5 - len(zip_code))) + zip_code

def fix_zip_code_columns(df, columns):
    for column in columns:
        df[column] = df.apply(lambda job: fix_zip_code(job[column]), axis=1)
    return df

# determines whether `job` is accurate based on its housing address geocoding results
# inaccurate iff (accuracy < 0.7 or accuracy type in bad_accuracy_types) and (state in our_states)
def is_accurate(job, housing_addendum=False):

    if housing_addendum:
        automatic_accurate_conditions = handle_null(job["HOUSING_STATE"]).lower() not in our_states
    elif job["Visa type"] == "H-2B":
        automatic_accurate_conditions = handle_null(job["WORKSITE_STATE"]).lower() not in our_states
    else:
        try:
            automatic_accurate_conditions = (handle_null(job["WORKSITE_STATE"]).lower() not in our_states) and (handle_null(job["HOUSING_STATE"]).lower() not in our_states)
        except:
            automatic_accurate_conditions = handle_null(job["WORKSITE_STATE"]).lower() not in our_states

    if state_checking and automatic_accurate_conditions:
        return True
    if job["table"] == "central":
        if job["Visa type"] == "H-2A":

            # check if this job is missing all its housing info
            # housing accuracy type won't be in job if job is missing housing info and all the other jobs in this run were H-2b or also missing housing info
            # otherwise (if there are jobs with housing info in the run) need to check whether all the housing address columns are null - if so, this job is missing all housing info
            if ("HOUSING_ADDRESS_LOCATION" not in job) or (pd.isna(job["HOUSING_ADDRESS_LOCATION"]) and pd.isna(job["HOUSING_CITY"]) and pd.isna(job["HOUSING_STATE"]) and pd.isna(job["HOUSING_POSTAL_CODE"])):
                if job['CASE_NUMBER'] not in h2as_without_housing:
                    print_red_and_email(f"{job['CASE_NUMBER']} is H-2A but doesn't have housing info.", "H-2A Job With No Housing Info")
                # return not ((job["worksite accuracy"] == None) or (job["worksite accuracy"] < 0.7) or (job["worksite accuracy type"] in bad_accuracy_types))
                # currently we don't want to correct worksites, so return true because there's nothing to fix
                return True
            # for an H2A row with housing info
            else:
                # return not ((not job["worksite accuracy type"]) or (not job["housing accuracy type"]) or (job["worksite accuracy"] < 0.7) or (job["housing accuracy"] < 0.7) or (job["worksite accuracy type"] in bad_accuracy_types) or (job["housing accuracy type"] in bad_accuracy_types))
                return not ( (not job["housing accuracy type"]) or (job["housing accuracy"] < 0.7) or (job["housing accuracy type"] in bad_accuracy_types) )

        elif job["Visa type"] == "H-2B":
            # return not ((job["worksite accuracy"] == None) or (job["worksite accuracy"] < 0.7) or (job["worksite accuracy type"] in bad_accuracy_types))
            # automatically true because h-2b jobs don't have housing addresses
            return True
        else:
            error_message = f"The `Visa type` column of this job -case number {job['CASE_NUMBER']}- was neither `H-2A` nor `H-2B`, marking as inaccurate."
            print_red_and_email(error_message, "Bad Visa Type")
            return False

    elif job["table"] == "dol_h":
        return not ((job["housing accuracy"] == None) or (job["housing accuracy"] < 0.7) or (job["housing accuracy type"] in bad_accuracy_types))

    else:
        error_message = f"The `table` column of this job -case number {job['CASE_NUMBER']}- was neither `dol_h` nor `central`"
        print_red_and_email(error_message, "Bad Table Column Value")
        return False

def h2a_or_h2b(job):
    if (job["CASE_NUMBER"][2] == "3") or (job["CASE_NUMBER"][0] == "3"):
        return "H-2A"
    elif (job["CASE_NUMBER"][2] == "4") or (job["CASE_NUMBER"][0] == "4"):
        return "H-2B"
    else:
        print_red_and_email(f"Case number: {job['CASE_NUMBER']}", "Case Number Malformed")
        return ""

# gets first value in column named column of dataframe `job` - normally `job` only have on row
def get_value(job, column):
    return job[column].tolist()[0]

def get_address_columns(worksite_or_housing):
    if worksite_or_housing == "worksite":
        return worksite_address_columns
    else:
        return housing_address_columns

# handles case of `new_job` being previously fixed, where `old_job` the row in the database with the same case number as `new_job`
def handle_previously_fixed(new_job, old_job, worksite_or_housing, set_fixed_to_false=False):
    fixed_by = old_job[f"{worksite_or_housing}_fixed_by"]
    if fixed_by in ["coordinates", "address"]:
        if fixed_by == "address":
            address_columns = get_address_columns(worksite_or_housing)
        else:
            address_columns = [f"{worksite_or_housing}_lat", f"{worksite_or_housing}_long", f"{worksite_or_housing} accuracy", f"{worksite_or_housing} accuracy type", f"{worksite_or_housing}_fixed_by", "fixed"]

        # overwrite all address column values in `new_job` with those in `old_job`
        for column in address_columns:
            new_job[column] = old_job[column]
    else:
        # if worksite_or_housing needs fixing in new_job and old_job wasn't fixed (since fixed_by wasn't coordinates or address), mark fixed as False
        if (pd.isna(new_job[f"{worksite_or_housing} accuracy type"])) or (new_job[f"{worksite_or_housing} accuracy"] < 0.7) or (new_job[f"{worksite_or_housing} accuracy type"] in bad_accuracy_types):
            new_job["fixed"] = False
            return new_job, False

    if set_fixed_to_false:
        new_job["fixed"] = False

    return new_job, True

def make_query(query):
    with engine.connect() as connection:
        result = connection.execute(query)
    return result

# returns list of all case nums from job_central if accurate, else from low_accuracies
def get_case_nums(accurate=None):
    if accurate:
        query_res = make_query('SELECT "CASE_NUMBER" FROM job_central')
    else:
        query_res = make_query("""SELECT "CASE_NUMBER" FROM low_accuracies WHERE "table" != 'dol_h'""")
    return [tup[0] for tup in list(query_res)]

# returns list of all column names from table_name
def get_columns(table_name):
    query_res = make_query(f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{table_name}'")
    return [column[0] for column in list(query_res)]

# adds columns in list `columns` to `table` with datatypes according to `column_types_dict`
def add_columns(table, columns, column_types_dict):
    for column in columns:
        myprint(f"Adding {column} as type {column_types_dict[column]} to {table}.")
        make_query(f'ALTER TABLE {table} ADD COLUMN "{column}" {column_types_dict[column]}')

# removes rows from `table` with case_num `case_number` and "table" column != dol_h (not additional housing rows)
def remove_case_num_from_table(case_number, table):
    if table == "job_central":
        make_query(f"""DELETE FROM job_central WHERE "CASE_NUMBER"='{case_number}'""")
    else:
        make_query(f"""DELETE FROM low_accuracies WHERE "CASE_NUMBER"='{case_number}' and "table" != 'dol_h'""")

# adds `job` to `table`
def add_job_to_postgres(job, table):
    job_df = pd.DataFrame(job.to_dict(), index=[0])

    job_columns = job_df.columns
    columns_to_drop = []

    # drop all columns that are in additional housing but not job_central
    # this is fine because this function is only ever called on non-additional_housing jobs
    if 'fy' in job_columns:
        columns_to_drop = list(set(get_columns("additional_housing")) - set(get_columns("job_central")))

    if "id" in job_columns:
        columns_to_drop.append("id")

    columns_to_drop = [column for column in columns_to_drop if column in job_df.columns]
    job_df = job_df.drop(columns=columns_to_drop)
    job_df.to_sql(table, engine, if_exists='append', index=False)

    # this can automatically add columns in job but not table to job, but I've decided we never want to let that just happen -
    # the database schema should remain constant unless we explicitly decide to change it
    # try:
    #     job_df.to_sql(table, engine, if_exists='append', index=False)
    #
    # except:
    #     columns_in_job_but_not_table = set(job_df.columns) - table_columns
    #     if columns_in_job_but_not_table:
    #
    #         opposite_table = "low_accuracies" if table == "job_central" else "job_central"
    #         these_column_types = [make_query(f"SELECT data_type FROM information_schema.columns WHERE table_name = '{opposite_table}' AND column_name = '{column}'").fetchall()[0][0] for column in columns_in_job_but_not_table]
    #         columns_and_types_dict = dict(zip(columns_in_job_but_not_table, these_column_types))
    #
    #         print_red_and_email(f"Adding columns to {table}. Here are the columns being added, with their corresponding types:\n{columns_and_types_dict}", "Adding columns!")
    #         add_columns(table, columns_in_job_but_not_table, columns_and_types_dict)
    #
    #     job_df.to_sql(table, engine, if_exists='append', index=False)

# adds data from the old_job (the job with `new_case_number` that already exists in postgres) to new_job
# returns updated new_job
def get_old_job_and_add_missing_columns(new_job, new_case_number, cols_in_both, cols_only_in_old, table):
    old_job_df = pd.read_sql(f"""SELECT * FROM {table} where "CASE_NUMBER"='{new_case_number}' and "table" != 'dol_h'""", con=engine)
    assert len(old_job_df) == 1

    old_job = old_job_df.iloc[0]
    # for each column that is in both new and old_job, if that column's value is null in new_job, give it the column's value in old_job
    for column in cols_in_both:
        if pd.isnull(new_job[column]):
            new_job[column] = old_job[column]

    # for each column that's only in the old job, add that column to new_job with the value from old_job
    for column in cols_only_in_old:
        new_job[column] = old_job[column]

    return old_job, new_job

# merges all data from the dataframe `jobs` into PostgreSQL
# `accurate` = True means `jobs` contains accurately geocoded jobs, False means inaccurate
# `old_accurate_case_nums` are all the case numbers in job_central
# `old_inaccurate_case_nums` are all the non additional_housing case numbers in low_accuracies
def merge_data(jobs, old_accurate_case_nums, old_inaccurate_case_nums, accurate=None):
    # should be no overlap between job_central and  (non additional_housing) low_accuracies case numbers
    assert not set(old_accurate_case_nums).intersection(old_inaccurate_case_nums)

    accurate_or_inaccurate = "accurate" if accurate else "inaccurate"

    jobs_columns = set(jobs.columns)
    accurate_columns = set(get_columns("job_central"))
    inaccurate_columns = set(get_columns("low_accuracies"))

    cols_not_in_accurate = jobs_columns - accurate_columns
    cols_not_in_inaccurate = jobs_columns - inaccurate_columns

    if cols_not_in_accurate or cols_not_in_inaccurate:
        not_in_acc_str, not_in_inacc_str = ", ".join(cols_not_in_accurate), ", ".join(cols_not_in_inaccurate)
        print_red_and_email(f"The following columns are in the new data but not in job_central: {not_in_acc_str}.\n\nThe following columns are in the new data but not in low_accuracies: {not_in_inacc_str}.", "Unidentified columns in new data!")
        raise Exception("Unidentified columns in new data.")

    cols_in_accurate_and_new = accurate_columns.intersection(jobs_columns)
    cols_in_inaccurate_and_new = inaccurate_columns.intersection(jobs_columns)

    cols_only_in_accurate = accurate_columns - jobs_columns
    cols_only_in_inaccurate = inaccurate_columns - jobs_columns

    for i, new_job in jobs.iterrows():

        # database table in which new_job will end up
        table_to_put = "job_central" if accurate else "low_accuracies"
        new_case_number = new_job["CASE_NUMBER"]
        if new_case_number in old_accurate_case_nums:
            myprint(f"DUPLICATE CASE NUMBER: {new_case_number} is in both the ({accurate_or_inaccurate}) new dataset and the accurates table in postgres.")
            old_job, new_job = get_old_job_and_add_missing_columns(new_job, new_case_number, cols_in_accurate_and_new, cols_only_in_accurate, "job_central")

            # if job with same case_number as new_jobs is in job_central but new_job is not accurate, make sure the old job was actually fixed, otherwise send new_job to low_accuracies
            if not accurate:
                new_job, worksite_fixed = handle_previously_fixed(new_job, old_job, "worksite")
                housing_fixed = True
                if new_job["Visa type"] == "H-2A":
                    # if worksite wasn't actually fixed and needed to be, keep fixed column value as False regardless of whether housing was
                    set_fixed_to_false = not worksite_fixed
                    new_job, housing_fixed = handle_previously_fixed(new_job, old_job, "housing", set_fixed_to_false=set_fixed_to_false)
                # new_job can go to job_central if both worskite and housing were either previously fixed or didn't need to be fixed
                if worksite_fixed and housing_fixed:
                    table_to_put = "job_central"

            remove_case_num_from_table(new_case_number, "job_central")

        elif new_case_number in old_inaccurate_case_nums:
            myprint(f"DUPLICATE CASE NUMBER: {new_case_number} is in both the ({accurate_or_inaccurate}) new dataset and the inaccurates table in postgres.")
            old_job, new_job = get_old_job_and_add_missing_columns(new_job, new_case_number, cols_in_inaccurate_and_new, cols_only_in_inaccurate, "low_accuracies")

            # if new_job is inaccurate and the old_job in low_accuracies has been fixed - but this should very rarely happen
            # to make it happen less run implement_fixes task before merging DOL data
            if (not accurate) and (old_job["fixed"]):
                new_job, worksite_fixed = handle_previously_fixed(new_job, old_job, "worksite")
                new_job, housing_fixed = handle_previously_fixed(new_job, old_job, "housing")

            remove_case_num_from_table(new_case_number, "low_accuracies")


        # uncomment if you want to use the part of add_job_to_postgres which will automatically add missing columns
        # table_to_put_columns = accurate_columns if table_to_put == "job_central" else inaccurate_columns
        # add_job_to_postgres(new_job, table_to_put, table_to_put_columns)

        add_job_to_postgres(new_job, table_to_put)


# merges data from the DataFrames `accurate_new_jobs` and `inaccurate_new_jobs` into our database
def merge_all_data(accurate_new_jobs, inaccurate_new_jobs):
    old_accurate_case_nums = get_case_nums(accurate=True)
    old_inaccurate_case_nums = get_case_nums(accurate=False)

    merge_data(accurate_new_jobs, old_accurate_case_nums, old_inaccurate_case_nums, accurate=True)
    merge_data(inaccurate_new_jobs, old_accurate_case_nums, old_inaccurate_case_nums, accurate=False)
