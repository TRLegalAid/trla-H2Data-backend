import os
from helpers import myprint
import helpers
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from geocodio import GeocodioClient
database_connection_string, geocodio_api_key, _, _, _, _, _ = helpers.get_secret_variables()
engine, client = create_engine(database_connection_string), GeocodioClient(geocodio_api_key)

def geocode_manage_split_merge(dol_jobs, accurate_old_jobs, inaccurate_old_jobs):
    # get dol data and postgres data (accurate and inaccurate), perform necessary data management on dol data
    dol_jobs = dol_jobs.drop_duplicates(subset='CASE_NUMBER', keep="last")
    helpers.fix_zip_code_columns(dol_jobs, ["HOUSING_POSTAL_CODE", "EMPLOYER_POC_POSTAL_CODE", "EMPLOYER_POSTAL_CODE", "WORKSITE_POSTAL_CODE", "ATTORNEY_AGENT_POSTAL_CODE"])
    dol_jobs["Source"], dol_jobs["table"] = "DOL", "central"
    dol_jobs["Date of run"] = datetime.today()
    dol_jobs["Job Order Link"] = dol_jobs.apply(lambda job: "https://seasonaljobs.dol.gov/job-order/" + job["CASE_NUMBER"], axis=1)
    dol_jobs['Visa type'] = dol_jobs.apply(lambda job: helpers.h2a_or_h2b(job), axis=1)
    columns_to_change_to_boolean = ["790A_ADDENDUM_B_ATTACHED", "790A_addendum_a_attached", "ADDENDUM_B_HOUSING_ATTACHED", "APPENDIX_A_ATTACHED",
                                    "CERTIFICATION_REQUIREMENTS", "CRIMINAL_BACKGROUND_CHECK", "DRIVER_REQUIREMENTS", "DRUG_SCREEN", "EMERGENCY_FILING",
                                    "FREQUENT_STOOPING_BENDING_OVER", "EXTENSIVE_SITTING_WALKING", "EXTENSIVE_PUSHING_PULLING", "EXPOSURE_TO_TEMPERATURES",
                                    "H-2A_LABOR_CONTRACTOR", "HOUSING_COMPLIANCE_FEDERAL", "HOUSING_COMPLIANCE_STATE", "HOUSING_COMPLIANCE_LOCAL", "HOUSING_TRANSPORTATION",
                                    "JOINT_EMPLOYER_APPENDIX_A_ATTACHED", "LIFTING_REQUIREMENTS", "MEALS_PROVIDED", "ON_CALL_REQUIREMENT", "REPETITIVE_MOVEMENTS",
                                    "SUPERVISE_OTHER_EMP", "SURETY_BOND_ATTACHED", "WORK_CONTRACTS_ATTACHED", "ADDENDUM_B_WORKSITE_ATTACHED"]

    def yes_no_to_boolean(yes_no):
        if yes_no.strip() == "Y":
            return True
        elif yes_no.strip() == "N":
            return False
        else:
            print_red_and_email("There was an error converting the multiple worksite value to a boolean.", "Error Converting to Boolean")
            return yes_no

    for column in columns_to_change_to_boolean:
        dol_jobs[column] = dol_jobs.apply(lambda job: yes_no_to_boolean(job[column]), axis=1)

    # geocode dol data and split by accuracy
    accurate_dol_jobs, inaccurate_dol_jobs = helpers.geocode_and_split_by_accuracy(dol_jobs)
    # merge all old and new data together
    accurate_jobs, inaccurate_jobs = helpers.merge_all_data(accurate_dol_jobs, inaccurate_dol_jobs, accurate_old_jobs, inaccurate_old_jobs)
    # sort data
    accurate_jobs, inaccurate_jobs = helpers.sort_df_by_date(accurate_jobs), helpers.sort_df_by_date(inaccurate_jobs)
    return accurate_jobs, inaccurate_jobs

def push_merged_to_sql():
    dol_jobs = pd.read_excel(os.path.join(os.getcwd(), '..', 'excel_files/all_dol_data.xlsx'), converters={'ATTORNEY_AGENT_PHONE':str,'PHONE_TO_APPLY':str, 'SOC_CODE': str, 'NAICS_CODE': str})
    accurate_old_jobs = pd.read_sql("job_central", con=engine)
    inaccurate_old_jobs = pd.read_sql("low_accuracies", con=engine)
    accurate_jobs, inaccurate_jobs = geocode_manage_split_merge(dol_jobs, accurate_old_jobs, inaccurate_old_jobs)
    accurate_jobs.to_sql("job_central", engine, if_exists='replace', index=False, dtype=helpers.column_types)
    inaccurate_jobs.to_sql("low_accuracies", engine, if_exists='replace', index=False, dtype=helpers.column_types)

if __name__ == "__main__":
   push_merged_to_sql()
