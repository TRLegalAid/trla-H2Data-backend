import os
from helpers import myprint
import helpers
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

def geocode_manage_split_merge(dol_jobs, h2a=True):

    dol_jobs = dol_jobs.drop_duplicates(subset='CASE_NUMBER', keep="last")

    if h2a:
        myprint(f"Merging H-2A DOL data.")
        table = "dol_h2a"
        dol_jobs = dol_jobs.rename(columns={"FREQUENCY_OF_PAY": "Additional Wage Information", "H2A_LABOR_CONTRACTOR": "H-2A_LABOR_CONTRACTOR",
                                            "HOURLY_WORK_SCHEDULE_START": "HOURLY_SCHEDULE_BEGIN", "HOURLY_WORK_SCHEDULE_END": "HOURLY_SCHEDULE_END",
                                            "TOTAL_WORKERS_H2A_CERTIFIED": "TOTAL_WORKERS_H-2A_CERTIFIED", "790A_ADDENDUM_A_ATTACHED": "790A_addendum_a_attached",
                                            "TOTAL_WORKERS_H2A_REQUESTED": "TOTAL_WORKERS_H-2A_REQUESTED"})

        helpers.fix_zip_code_columns(dol_jobs, ["HOUSING_POSTAL_CODE", "EMPLOYER_POC_POSTAL_CODE", "EMPLOYER_POSTAL_CODE", "WORKSITE_POSTAL_CODE", "ATTORNEY_AGENT_POSTAL_CODE"])

        columns_to_change_to_boolean = ["790A_ADDENDUM_B_ATTACHED", "790A_addendum_a_attached", "ADDENDUM_B_HOUSING_ATTACHED", "APPENDIX_A_ATTACHED",
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
            return False
        stripped_val = yes_no.strip()
        if stripped_val in ["Y", "Yes", "YES"]:
            return True
        elif stripped_val in ["N", "No", "NO", "N/A"]:
            return False
        else:
            helpers.print_red_and_email(f"There was an error converting {stripped_val} to boolean.", "Error Converting to Boolean")
            return yes_no

    for column in columns_to_change_to_boolean:
        dol_jobs[column] = dol_jobs.apply(lambda job: yes_no_to_boolean(job[column]), axis=1)

    # geocode dol data and split by accuracy
    myprint(f"There are {len(dol_jobs)} DOL jobs to merge.")
    accurate_dol_jobs, inaccurate_dol_jobs = helpers.geocode_and_split_by_accuracy(dol_jobs, table=table)
    helpers.merge_all_data(accurate_dol_jobs, inaccurate_dol_jobs)

def push_merged_to_sql():
    dol_file_path = "dol_data/H2B_Disclosure_Data_FY2020.xlsx"
    dol_jobs = pd.read_excel(dol_file_path, converters={'ATTORNEY_AGENT_PHONE':str,'ATTORNEY_AGENT_PHONE_EXT':str, 'PHONE_TO_APPLY':str,
                                                                                        'SOC_CODE': str, 'NAICS_CODE': str, 'EMPLOYER_POC_PHONE': str, 'EMPLOYER_PHONE': str,
                                                                                        'EMPLOYER_POC_PHONE_EXT': str, 'EMPLOYER_PHONE_EXT': str})
    h2a_response = input("Is this for H2A? Enter Y or N: ")
    is_h2a = h2a_response.lower() in ["y", "yes"]
    geocode_manage_split_merge(dol_jobs, h2a=is_h2a)

if __name__ == "__main__":
   push_merged_to_sql()
