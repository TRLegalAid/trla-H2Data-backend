import os
from helpers import myprint
import helpers
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from geocodio import GeocodioClient
import time
database_connection_string, geocodio_api_key, _, _, _, _, _, _ = helpers.get_secret_variables()
engine, client = create_engine(database_connection_string), GeocodioClient(geocodio_api_key)

def geocode_manage_split_merge(dol_jobs, accurate_old_jobs, inaccurate_old_jobs):
    # get dol data and postgres data (accurate and inaccurate), perform necessary data management on dol data
    dol_jobs = dol_jobs.drop_duplicates(subset='CASE_NUMBER', keep="last")
    dol_jobs = dol_jobs.rename(columns={"TOTAL_WORKERS_REQUESTED": "TOTAL_WORKERS_NEEDED", "WORKSITE_ADDRESS1": "WORKSITE_ADDRESS", "SPECIAL_REQUIREMENTS": "ADDITIONAL_JOB_REQUIREMENTS", "ADDITIONAL_WAGE_CONDITIONS": "Additional Wage Information"})

    helpers.fix_zip_code_columns(dol_jobs, ["EMPLOYER_POC_POSTAL_CODE", "EMPLOYER_POSTAL_CODE", "WORKSITE_POSTAL_CODE", "ATTORNEY_AGENT_POSTAL_CODE"])
    dol_jobs["Source"], dol_jobs["table"] = "DOL", "central"
    dol_jobs["Date of run"] = datetime.today()
    dol_jobs["Job Order Link"] = dol_jobs.apply(lambda job: "https://seasonaljobs.dol.gov/job-order/" + job["CASE_NUMBER"], axis=1)
    dol_jobs['Visa type'] = dol_jobs.apply(lambda job: helpers.h2a_or_h2b(job), axis=1)
    columns_to_change_to_boolean = ["EMERGENCY_FILING_PWD_ATTACHED", "CAP_EXEMPT", "OTHER_WORKSITE_LOCATION", "DAILY_TRANSPORTATION",
                                    "SUPERVISE_OTHER_EMP", "OVERTIME_AVAILABLE", "ON_THE_JOB_TRAINING_AVAILABLE",
                                    "EMP_PROVIDED_TOOLS_EQUIPMENT", "BOARD_LODGING_OTHER_FACILITIES", "APPENDIX_D_COMPLETED",
                                    "FOREIGN_LABOR_RECRUITER", "EMPLOYER_APPENDIX_B_ATTACHED", "EMP_CLIENT_APPENDIX_B_ATTACHED"]

    def yes_no_to_boolean(yes_no):
        if pd.isnull(yes_no):
            return False
        stripped_val = yes_no.strip()
        if stripped_val == "Y":
            return True
        elif stripped_val == "N" or stripped_val == "N/A":
            return False
        else:
            print_red_and_email(f"There was an error converting {stripped_val} to boolean.", "Error Converting to Boolean")
            return yes_no

    for column in columns_to_change_to_boolean:
        dol_jobs[column] = dol_jobs.apply(lambda job: yes_no_to_boolean(job[column]), axis=1)

    # geocode dol data and split by accuracy
    accurate_dol_jobs, inaccurate_dol_jobs = helpers.geocode_and_split_by_accuracy(dol_jobs, table="dol_h2b")
    # merge all old and new data together
    accurate_jobs, inaccurate_jobs = helpers.merge_all_data(accurate_dol_jobs, inaccurate_dol_jobs, accurate_old_jobs, inaccurate_old_jobs)
    # sort data
    accurate_jobs, inaccurate_jobs = helpers.sort_df_by_date(accurate_jobs), helpers.sort_df_by_date(inaccurate_jobs)
    return accurate_jobs, inaccurate_jobs

def push_merged_to_sql():
    beginning = time.time()
    dol_jobs = pd.read_excel(os.path.join(os.getcwd(), '..', 'excel_files/h2b_q3.xlsx'), converters={'ATTORNEY_AGENT_PHONE':str,'ATTORNEY_AGENT_PHONE_EXT':str, 'PHONE_TO_APPLY':str,
                                                                                                        'SOC_CODE': str, 'NAICS_CODE': str, 'EMPLOYER_POC_PHONE': str, 'EMPLOYER_PHONE': str,
                                                                                                        'EMPLOYER_POC_PHONE_EXT': str, 'EMPLOYER_PHONE_EXT': str})

    myprint(f"Finished reading table after {time.time() - beginning} seconds.")

    accurate_old_jobs = pd.read_sql("job_central", con=engine)
    inaccurate_old_jobs = pd.read_sql("low_accuracies", con=engine)
    myprint(f"Finished reading sql tables after {time.time() - beginning} seconds.")

    accurate_jobs, inaccurate_jobs = geocode_manage_split_merge(dol_jobs, accurate_old_jobs, inaccurate_old_jobs)
    myprint(f"Finished geocode_manage_split_merge after {time.time() - beginning} seconds.")

    accurate_jobs.to_excel(f"accurate_backup_{time.time()}.xlsx")
    inaccurate_jobs.to_excel(f"inaccurate_backup_{time.time()}.xlsx")
    myprint(f"Finished backing up to excel after {time.time() - beginning} seconds.")

    accurate_jobs.to_sql("job_central", engine, if_exists='replace', index=False, dtype=helpers.column_types)
    myprint(f"Finished pushing job central to SQL after {time.time() - beginning} seconds.")

    inaccurate_jobs.to_sql("low_accuracies", engine, if_exists='replace', index=False, dtype=helpers.column_types)
    myprint(f"Finished pushing low accuracies to SQL after {time.time() - beginning} seconds.")


if __name__ == "__main__":
   push_merged_to_sql()
