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
geocodio_api_key = os.getenv("GEOCODIO_API_KEY")
client = GeocodioClient(geocodio_api_key)

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
    if os.getenv("LOCAL_DEV") == "true":
        return
    email, password = os.getenv("ERROR_EMAIL_ADDRESS"), os.getenv("ERROR_EMAIL_ADDRESS_PASSWORD")
    port, smtp_server, context  = 465, "smtp.gmail.com", ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(email, password)
        server.sendmail(email, email, message)

def print_red_and_email(message, subject):
    frameinfo = getframeinfo((stack()[1][0]))
    file_name_and_line_info = "(" + frameinfo.filename.split("/")[-1] + ", line " + str(frameinfo.lineno) + ")"
    myprint(message + "  " + file_name_and_line_info, is_red="red", email_also="yes")
    send_email("Subject: " + subject + "\n\n" + message + "\n" + file_name_and_line_info)

def get_database_engine(force_cloud=False):
    if force_cloud or (not (os.getenv("LOCAL_DEV") == "true")):
        return create_engine(os.getenv("DATABASE_URL"))
    else:
        return create_engine(os.getenv("LOCAL_DATABASE_URL"))

# set to True to run real tasks locally
force_cloud = True
engine = get_database_engine(force_cloud=force_cloud)

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
    "790A_ADDENDUM_B_ATTACHED": sqlalchemy.types.Boolean, "OTHER_WORKSITE_LOCATION": sqlalchemy.types.Boolean, "SUPERVISE_OTHER_EMP": sqlalchemy.types.Boolean,
    "790A_addendum_a_attached": sqlalchemy.types.Boolean,"ADDENDUM_B_HOUSING_ATTACHED": sqlalchemy.types.Boolean, "APPENDIX_A_ATTACHED": sqlalchemy.types.Boolean, "CRIMINAL_BACKGROUND_CHECK": sqlalchemy.types.Boolean,
    "DRIVER_REQUIREMENTS": sqlalchemy.types.Boolean, "DRUG_SCREEN": sqlalchemy.types.Boolean, "EMERGENCY_FILING": sqlalchemy.types.Boolean, "EXTENSIVE_SITTING_WALKING": sqlalchemy.types.Boolean,
    "EXTENSIVE_PUSHING_PULLING": sqlalchemy.types.Boolean, "EXPOSURE_TO_TEMPERATURES": sqlalchemy.types.Boolean, "FREQUENT_STOOPING_BENDING_OVER": sqlalchemy.types.Boolean,
    "H-2A_LABOR_CONTRACTOR": sqlalchemy.types.Boolean, "HOUSING_COMPLIANCE_FEDERAL": sqlalchemy.types.Boolean, "HOUSING_COMPLIANCE_STATE": sqlalchemy.types.Boolean, "HOUSING_COMPLIANCE_LOCAL": sqlalchemy.types.Boolean,
    "HOUSING_TRANSPORTATION": sqlalchemy.types.Boolean, "JOINT_EMPLOYER_APPENDIX_A_ATTACHED": sqlalchemy.types.Boolean, "LIFTING_REQUIREMENTS": sqlalchemy.types.Boolean, "MEALS_PROVIDED": sqlalchemy.types.Boolean,
    "ON_CALL_REQUIREMENT": sqlalchemy.types.Boolean, "REPETITIVE_MOVEMENTS": sqlalchemy.types.Boolean, "SURETY_BOND_ATTACHED": sqlalchemy.types.Boolean, "WORK_CONTRACTS_ATTACHED": sqlalchemy.types.Boolean,
    "CERTIFICATION_REQUIREMENTS": sqlalchemy.types.Boolean, "DECISION_DATE": sqlalchemy.types.DateTime
}
housing_address_columns = ["HOUSING_ADDRESS_LOCATION", "HOUSING_CITY", "HOUSING_STATE", "HOUSING_POSTAL_CODE", "housing_lat", "housing_long", "housing accuracy", "housing accuracy type", "housing_fixed_by", "fixed"]
worksite_address_columns = ["WORKSITE_ADDRESS", "WORKSITE_CITY", "WORKSITE_STATE", "WORKSITE_POSTAL_CODE", "worksite_lat", "worksite_long", "worksite accuracy", "worksite accuracy type", "worksite_fixed_by", "fixed"]
h2as_without_housing = ["H-300-20306-894174", "H-300-20293-882133", "H-300-20288-878041"]

# function for printing dictionary
def prettier(dictionary):
    for key in dictionary:
        print(key, ": ", dictionary[key])

def create_address_from(address, city, state, zip):
    try:
        return address + ", " + city + " " + state + " " + str(zip)
    except:
        return ""

def convert_date_to_string(datetime):
    if pd.isna(datetime):
        return ""
    return datetime.strftime("%m/%d/%Y, %H:%M:%S")

def geocode_table(df, worksite_or_housing):
    myprint(f"Geocoding {worksite_or_housing}...")

    if worksite_or_housing == "worksite":
        geocoding_type = "worksite"
        addresses = df.apply(lambda job: create_address_from(job["WORKSITE_ADDRESS"], job["WORKSITE_CITY"], job["WORKSITE_STATE"], job["WORKSITE_POSTAL_CODE"]), axis=1).tolist()
    elif worksite_or_housing == "housing":
        geocoding_type = "housing"
        addresses = df.apply(lambda job: create_address_from(job["HOUSING_ADDRESS_LOCATION"], job["HOUSING_CITY"], job["HOUSING_STATE"], job["HOUSING_POSTAL_CODE"]), axis=1).tolist()
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

    now = datetime.now(tz=timezone('US/Eastern')).strftime("%I.%M%.%S_%p_%B_%d_%Y")
    # df.to_excel(f"../geocoding_backups/{now}.xlsx")
    # myprint("Backed up geocoding results")

    return df

def geocode_and_split_by_accuracy(df, table=""):
    if table == "dol_h2b":
        df = geocode_table(df, "worksite")
    elif table == "housing addendum":
        df = geocode_table(df, "housing")
    else:
        df = geocode_table(df, "worksite")
        if "HOUSING_ADDRESS_LOCATION" in df.columns:
            df = geocode_table(df, "housing")
        else:
            print_red_and_email("Not geocoding housing because HOUSING_ADDRESS_LOCATION is not present. This should be fine, and hopefully just means there were only H-2B jobs in today's run, but you may want to check.", "Not geocoding housing today")
            # pass

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

def is_accurate(job, housing_addendum=False):

    if housing_addendum:
        automatic_accurate_conditions = job["HOUSING_STATE"].lower() not in our_states
    elif job["Visa type"] == "H-2B":
        automatic_accurate_conditions = job["WORKSITE_STATE"].lower() not in our_states
    else:
        automatic_accurate_conditions = (job["WORKSITE_STATE"].lower() not in our_states) and (job["HOUSING_STATE"].lower() not in our_states)

    if state_checking and automatic_accurate_conditions:
        return True
    if job["table"] == "central":
        if job["Visa type"] == "H-2A":

            # check if this job is missing all its housing info
            # housing accuracy type won't be in job if job is missing housing info and all the other jobs in this run were H-2b or also missing housing info
            # otherwise need to check whether all the housing address columns are null - if so, this job is missing all housing info
            if ("HOUSING_ADDRESS_LOCATION" not in job) or (pd.isna(job["HOUSING_ADDRESS_LOCATION"]) and pd.isna(job["HOUSING_CITY"]) and pd.isna(job["HOUSING_STATE"]) and pd.isna(job["HOUSING_POSTAL_CODE"])):
                if job['CASE_NUMBER'] not in h2as_without_housing:
                    print_red_and_email(f"{job['CASE_NUMBER']} is H-2A but doesn't have housing info.", "H-2A Job With No Housing Info")
                return not ((job["worksite accuracy"] == None) or (job["worksite accuracy"] < 0.7) or (job["worksite accuracy type"] in bad_accuracy_types))
            # for an H2A row with housing info
            else:
                return not ((not job["worksite accuracy type"]) or (not job["housing accuracy type"]) or (job["worksite accuracy"] < 0.7) or (job["housing accuracy"] < 0.7) or (job["worksite accuracy type"] in bad_accuracy_types) or (job["housing accuracy type"] in bad_accuracy_types))
        elif job["Visa type"] == "H-2B":
            return not ((job["worksite accuracy"] == None) or (job["worksite accuracy"] < 0.7) or (job["worksite accuracy type"] in bad_accuracy_types))
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

def handle_previously_fixed(new_job, old_job, worksite_or_housing, set_fixed_to_false=False):
    fixed_by = old_job[f"{worksite_or_housing}_fixed_by"]
    if fixed_by in ["coordinates", "address"]:
        if fixed_by == "address":
            address_columns = get_address_columns(worksite_or_housing)
        else:
            address_columns = [f"{worksite_or_housing}_lat", f"{worksite_or_housing}_long", f"{worksite_or_housing} accuracy", f"{worksite_or_housing} accuracy type", f"{worksite_or_housing}_fixed_by", "fixed"]

        for column in address_columns:
            new_job[column] = old_job[column]
    else:
        # if worksite_or_housing needs fixing in new df, mark as False
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

def get_case_nums(accurate=None):
    if accurate:
        query_res = make_query('SELECT "CASE_NUMBER" FROM job_central')
    else:
        query_res = make_query("""SELECT "CASE_NUMBER" FROM low_accuracies WHERE "table" != 'dol_h'""")
    return [tup[0] for tup in list(query_res)]

def get_columns(table_name):
    query_res = make_query(f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{table_name}'")
    return [column[0] for column in list(query_res)]

def add_columns(table, columns, column_types_dict):
    for column in columns:
        myprint(f"Adding {column} as type {column_types_dict[column]} to {table}.")
        make_query(f'ALTER TABLE {table} ADD COLUMN "{column}" {column_types_dict[column]}')

def remove_case_num_from_table(case_number, table):
    if table == "job_central":
        make_query(f"""DELETE FROM job_central WHERE "CASE_NUMBER"='{case_number}'""")
    else:
        make_query(f"""DELETE FROM low_accuracies WHERE "CASE_NUMBER"='{case_number}' and "table" != 'dol_h'""")

def add_job_to_postgres(job, table, table_columns):
    job_df = pd.DataFrame(job.to_dict(), index=[0])
    try:
        job_df.to_sql(table, engine, if_exists='append', index=False, dtype=column_types)

    except:
        columns_in_job_but_not_table = set(job_df.columns) - table_columns
        if columns_in_job_but_not_table:

            opposite_table = "low_accuracies" if table == "job_central" else "job_central"
            these_column_types = [make_query(f"SELECT data_type FROM information_schema.columns WHERE table_name = '{opposite_table}' AND column_name = '{column}'").fetchall()[0][0] for column in columns_in_job_but_not_table]
            columns_and_types_dict = dict(zip(columns_in_job_but_not_table, these_column_types))

            print_red_and_email(f"Adding columns to {table}. Here are the columns being added, with their corresponding types:\n{columns_and_types_dict}", "Adding columns!")
            add_columns(table, columns_in_job_but_not_table, columns_and_types_dict)

        job_df.to_sql(table, engine, if_exists='append', index=False, dtype=column_types)


def sort_df_by_date(df):
    return df.sort_values(by=["RECEIVED_DATE"], ascending=True)

def get_old_job_and_add_missing_columns(new_job, new_case_number, cols_in_both, cols_only_in_old, table):
    old_job_df = pd.read_sql(f"""SELECT * FROM {table} where "CASE_NUMBER"='{new_case_number}' and "table" != 'dol_h'""", con=engine)
    assert len(old_job_df) == 1

    old_job = old_job_df.iloc[0]
    for column in cols_in_both:
        if pd.isna(new_job[column]):
            new_job[column] = old_job[column]
    for column in cols_only_in_old:
        new_job[column] = old_job[column]

    return old_job, new_job

def merge_data(jobs, old_accurate_case_nums, old_inaccurate_case_nums, accurate=None):
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

        table_to_put = "job_central" if accurate else "low_accuracies"
        new_case_number = new_job["CASE_NUMBER"]
        if new_case_number in old_accurate_case_nums:
            myprint(f"DUPLICATE CASE NUMBER: {new_case_number} is in both the ({accurate_or_inaccurate}) new dataset and the accurates table in postgres.")
            old_job, new_job = get_old_job_and_add_missing_columns(new_job, new_case_number, cols_in_accurate_and_new, cols_only_in_accurate, "job_central")

            if not accurate:
                new_job, worksite_fixed = handle_previously_fixed(new_job, old_job, "worksite")
                housing_fixed = True
                if new_job["Visa type"] == "H-2A":
                    set_fixed_to_false = not worksite_fixed
                    new_job, housing_fixed = handle_previously_fixed(new_job, old_job, "housing", set_fixed_to_false=set_fixed_to_false)
                if worksite_fixed and housing_fixed:
                    table_to_put = "job_central"

            remove_case_num_from_table(new_case_number, "job_central")

        elif new_case_number in old_inaccurate_case_nums:
            myprint(f"DUPLICATE CASE NUMBER: {new_case_number} is in both the ({accurate_or_inaccurate}) new dataset and the inaccurates table in postgres.")
            old_job, new_job = get_old_job_and_add_missing_columns(new_job, new_case_number, cols_in_inaccurate_and_new, cols_only_in_inaccurate, "low_accuracies")

            if (not accurate) and (old_job["fixed"]):
                new_job, worksite_fixed = handle_previously_fixed(new_job, old_job, "worksite")
                new_job, housing_fixed = handle_previously_fixed(new_job, old_job, "housing")

            remove_case_num_from_table(new_case_number, "low_accuracies")


        table_to_put_columns = accurate_columns if table_to_put == "job_central" else inaccurate_columns
        add_job_to_postgres(new_job, table_to_put, table_to_put_columns)


def merge_all_data(accurate_new_jobs, inaccurate_new_jobs):
    old_accurate_case_nums = get_case_nums(accurate=True)
    old_inaccurate_case_nums = get_case_nums(accurate=False)

    merge_data(accurate_new_jobs, old_accurate_case_nums, old_inaccurate_case_nums, accurate=True)
    merge_data(inaccurate_new_jobs, old_accurate_case_nums, old_inaccurate_case_nums, accurate=False)
