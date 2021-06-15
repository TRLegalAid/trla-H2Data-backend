"""Script to merge DOL's H-2A and H-2B disclosure data into our database."""

import os
from helpers import myprint
import helpers
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

# dol_jobs is a DataFrame containing the dol dataset
def geocode_manage_split_merge(dol_jobs, h2a=True):

    dol_jobs = dol_jobs.drop_duplicates(subset='CASE_NUMBER', keep="last")

    if h2a:
        myprint(f"Merging H-2A DOL data.")
        table = "dol_h2a"

        # rename columns to match our database
        dol_jobs = dol_jobs.rename(columns={"FREQUENCY_OF_PAY": "Additional Wage Information", "H2A_LABOR_CONTRACTOR": "H-2A_LABOR_CONTRACTOR",
                                            "HOURLY_WORK_SCHEDULE_START": "HOURLY_SCHEDULE_BEGIN", "HOURLY_WORK_SCHEDULE_END": "HOURLY_SCHEDULE_END",
                                            "TOTAL_WORKERS_H2A_CERTIFIED": "TOTAL_WORKERS_H-2A_CERTIFIED", "790A_ADDENDUM_A_ATTACHED": "790A_addendum_a_attached",
                                            "TOTAL_WORKERS_H2A_REQUESTED": "TOTAL_WORKERS_H-2A_REQUESTED"})

        # append 0's where necessary to zip code columns
        helpers.fix_zip_code_columns(dol_jobs, ["HOUSING_POSTAL_CODE", "EMPLOYER_POC_POSTAL_CODE", "EMPLOYER_POSTAL_CODE", "WORKSITE_POSTAL_CODE", "ATTORNEY_AGENT_POSTAL_CODE"])

        columns_to_change_to_boolean = ["790A_ADDENDUM_B_ATTACHED", "790A_addendum_a_attached", "ADDENDUM_B_HOUSING_ATTACHED", "APPENDIX_A_ATTACHED", "ADDENDUM_C_ATTACHED",
                                        "CERTIFICATION_REQUIREMENTS", "CRIMINAL_BACKGROUND_CHECK", "DRIVER_REQUIREMENTS", "DRUG_SCREEN", "EMERGENCY_FILING",
                                        "FREQUENT_STOOPING_BENDING_OVER", "EXTENSIVE_SITTING_WALKING", "EXTENSIVE_PUSHING_PULLING", "EXPOSURE_TO_TEMPERATURES",
                                        "H-2A_LABOR_CONTRACTOR", "HOUSING_COMPLIANCE_FEDERAL", "HOUSING_COMPLIANCE_STATE", "HOUSING_COMPLIANCE_LOCAL", "HOUSING_TRANSPORTATION",
                                        "JOINT_EMPLOYER_APPENDIX_A_ATTACHED", "LIFTING_REQUIREMENTS", "MEALS_PROVIDED", "ON_CALL_REQUIREMENT", "REPETITIVE_MOVEMENTS",
                                        "SUPERVISE_OTHER_EMP", "SURETY_BOND_ATTACHED", "WORK_CONTRACTS_ATTACHED", "ADDENDUM_B_WORKSITE_ATTACHED"]

    else:
        myprint(f"Merging H-2B DOL data.")
        table = "dol_h2b"
        dol_jobs = dol_jobs.rename(columns={"TOTAL_WORKERS_REQUESTED": "TOTAL_WORKERS_NEEDED", "WORKSITE_ADDRESS1": "WORKSITE_ADDRESS",
                                            "SPECIAL_REQUIREMENTS": "ADDITIONAL_JOB_REQUIREMENTS", "ADDITIONAL_WAGE_CONDITIONS": "Additional Wage Information",
                                            "ATTORNEY_AGENT_ADDRESS2": "ATTORNEY_AGENT_ADDRESS_2", "ATTORNEY_AGENT_ADDRESS1": "ATTORNEY_AGENT_ADDRESS_1"})

        helpers.fix_zip_code_columns(dol_jobs, ["EMPLOYER_POC_POSTAL_CODE", "EMPLOYER_POSTAL_CODE", "WORKSITE_POSTAL_CODE", "ATTORNEY_AGENT_POSTAL_CODE"])

        columns_to_change_to_boolean = ["EMERGENCY_FILING_PWD_ATTACHED", "CAP_EXEMPT", "OTHER_WORKSITE_LOCATION", "DAILY_TRANSPORTATION",
                                        "SUPERVISE_OTHER_EMP", "OVERTIME_AVAILABLE", "ON_THE_JOB_TRAINING_AVAILABLE",
                                        "EMP_PROVIDED_TOOLS_EQUIPMENT", "BOARD_LODGING_OTHER_FACILITIES", "APPENDIX_D_COMPLETED",
                                        "FOREIGN_LABOR_RECRUITER", "EMPLOYER_APPENDIX_B_ATTACHED", "EMP_CLIENT_APPENDIX_B_ATTACHED"]


    dol_jobs["Source"], dol_jobs["table"] = "DOL", "central"
    dol_jobs["Date of run"] = datetime.today()
    dol_jobs["Job Order Link"] = dol_jobs.apply(lambda job: "https://seasonaljobs.dol.gov/job-order/" + job["CASE_NUMBER"], axis=1)
    dol_jobs['Visa type'] = dol_jobs.apply(lambda job: helpers.h2a_or_h2b(job), axis=1)


    def yes_no_to_boolean(yes_no):
        if pd.isnull(yes_no):
            return None
        stripped_val = yes_no.strip()
        if stripped_val in ["Y", "Yes", "YES", "yes"]:
            return True
        elif stripped_val in ["N", "No", "NO", "no", "N/A"]:
            return False
        else:
            helpers.print_red_and_email(f"There was an error converting {stripped_val} to boolean.", "Error Converting to Boolean")
            exit()

    # change appropriate columns to boolean
    for column in columns_to_change_to_boolean:
        dol_jobs[column] = dol_jobs.apply(lambda job: yes_no_to_boolean(job[column]), axis=1)

    # geocode data, split by accuracy, merge into database
    myprint(f"There are {len(dol_jobs)} DOL jobs to merge.")
    accurate_dol_jobs, inaccurate_dol_jobs = helpers.geocode_and_split_by_accuracy(dol_jobs, table=table)
    helpers.merge_all_data(accurate_dol_jobs, inaccurate_dol_jobs)

# merges DOL data from dataset specified by user input
def merge_data():

    h2a_response = input("Are you merging DOL data for H-2A or H-2B? Enter `a` for H2A, `b` for H2B.\n").strip().lower()
    if h2a_response == "a":
        is_h2a = True
        h2a_or_h2b = "H-2A"
    elif h2a_response == "b":
        is_h2a = False
        h2a_or_h2b = "H-2B"
    else:
        raise ValueError("The answer to the last question must be either A or B, nothing else!")

    file_path = "dol_data/" + input(f"Check that the DOL data file is in a folder named `dol_data` in the `scripts` folder. (If it isn't, exit this using control + c then re-run this script once you've done it.) Now enter the full name (including any extension) of the DOL file (this is case sensitive).\n").strip()
    input(f"Ok, merging {h2a_or_h2b} DOL data from {file_path}. If this is correct press any key, othewise press control + c to start over.")

    dol_jobs = pd.read_excel(file_path, converters={'ATTORNEY_AGENT_PHONE':str,'ATTORNEY_AGENT_PHONE_EXT':str, 'PHONE_TO_APPLY':str,
                                                                                        'SOC_CODE': str, 'NAICS_CODE': str, 'EMPLOYER_POC_PHONE': str, 'EMPLOYER_PHONE': str,
                                                                                        'EMPLOYER_POC_PHONE_EXT': str, 'EMPLOYER_PHONE_EXT': str})
    geocode_manage_split_merge(dol_jobs, h2a=is_h2a)
    myprint("Done.")

if __name__ == "__main__":
   merge_data()
