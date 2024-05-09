"""Functions to mark jobs in low_accuracies that are ended (according to their status columns) as fixed."""

import pandas as pd
from helpers import get_database_engine, make_query, myprint
from sqlalchemy.sql import text
engine = get_database_engine(force_cloud=True)

def mark_inactive_central_as_fixed():
    make_query("""UPDATE low_accuracies SET
                    fixed = true,
                    worksite_fixed_by = 'inactive',
                    housing_fixed_by = 'inactive'
                    WHERE status = 'ended'""")

def mark_inactive_dolH_as_fixed():
    acc_case_nums_statuses_df = pd.read_sql("""select "CASE_NUMBER", status from job_central""", con=engine)
    inacc_case_nums_statuses_df = pd.read_sql("""select "CASE_NUMBER", status from low_accuracies where "table" != 'dol_h'""", con=engine)

    # check that there are no duplicate case numbers
    case_nums_list = acc_case_nums_statuses_df["CASE_NUMBER"].tolist() + inacc_case_nums_statuses_df["CASE_NUMBER"].tolist()
    assert len(case_nums_list) == len(set(case_nums_list))

    all_case_nums_statuses_df = pd.concat([acc_case_nums_statuses_df, inacc_case_nums_statuses_df])

    inaccurate_additional_housings = pd.read_sql("""select "id", "CASE_NUMBER" from low_accuracies where "table" = 'dol_h'""", con=engine)
    myprint(f"There are {len(inaccurate_additional_housings)} inaccurate additional housing rows.")

    case_nums_with_no_matches = []
    num_fixed = 0
    # iterate over all inaccurate additional housing case numbers and ids. if a rows's corresponding row in job_central is inactive, update the row's fixed column to be true
    for i, job in inaccurate_additional_housings.iterrows():
        job_case_num = job["CASE_NUMBER"]
        central_job = all_case_nums_statuses_df[all_case_nums_statuses_df["CASE_NUMBER"] == job_case_num]

        if len(central_job) > 1:
            myprint(f"{job_case_num} has {len(central_job)} matching rows in the dataframe with all non dol_h case numbers. This should be impossible.")
        elif len(central_job) == 0:
            myprint(f"{job_case_num} has no matching rows in the dataframe with all non dol_h case numbers.")
            # case_nums_with_no_matches.append(job_case_num)
            case_nums_with_no_matches = pd.concat([case_nums_with_no_matches, job_case_num])
        else:
            job_status = central_job["status"].tolist()[0]
            if job_status not in ["not yet started", "started"]:
                job_id = job["id"]
                make_query(f"""UPDATE low_accuracies SET
                                fixed = true,
                                worksite_fixed_by = 'inactive',
                                housing_fixed_by = 'inactive'
                                WHERE "id" = {job_id}""")
                num_fixed += 1

    # myprint(f"There are {len(case_nums_with_no_matches)} additional housing rows in low_accuracies without a matching central case number.")
    # myprint(f"There are {len(set(case_nums_with_no_matches))} additional housing unique case numbers in low_accuracies without a matching central case number.")
    myprint(f"{num_fixed} additional housing rows in low accuracies were marked as fixed.")

def mark_all_inactive_low_accurates_as_fixed():
    mark_inactive_central_as_fixed()
    mark_inactive_dolH_as_fixed()

if __name__ == "__main__":
    mark_all_inactive_low_accurates_as_fixed()
