import os
import helpers
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine
from geocodio import GeocodioClient
database_connection_string, geocodio_api_key = helpers.get_secret_variables()
engine, client = create_engine(database_connection_string), GeocodioClient(geocodio_api_key)

# get dol data and postgres data (accurate and inaccurate), perform necessary data management on dol data
dol_jobs = pd.read_excel(os.path.join(os.getcwd(), '..', 'excel_files/dol_data.xlsx'), converters={'ATTORNEY_AGENT_PHONE':str,'PHONE_TO_APPLY':str})
accurate_old_jobs = pd.read_sql("job_central", con=engine)
inaccurate_old_jobs = pd.read_sql("low_accuracies", con=engine)
dol_jobs = dol_jobs.drop_duplicates(subset='CASE_NUMBER', keep="last")
helpers.fix_zip_code_columns(dol_jobs, ["HOUSING_POSTAL_CODE", "EMPLOYER_POC_POSTAL_CODE", "EMPLOYER_POSTAL_CODE", "WORKSITE_POSTAL_CODE", "ATTORNEY_AGENT_POSTAL_CODE"])
dol_jobs["Source"], dol_jobs["table"] = "DOL", "central"
dol_jobs["Job Order Link"] = dol_jobs.apply(lambda job: "https://seasonaljobs.dol.gov/job-order/" + job["CASE_NUMBER"], axis=1)
dol_jobs['Visa type'] = dol_jobs.apply(lambda job: helpers.h2a_or_h2b(job), axis=1)
def yes_no_to_boolean(yes_no):
    if yes_no.strip() == "Y":
        return True
    elif yes_no.strip() == "N":
        return False
    else:
        print("there was an error converting the multiple worksite value to a boolean \n")
        return yes_no
dol_jobs["ADDENDUM_B_WORKSITE_ATTACHED"] = dol_jobs.apply(lambda job: yes_no_to_boolean(job["ADDENDUM_B_WORKSITE_ATTACHED"]), axis=1)

# geocode dol data and split by accuracy
accurate_dol_jobs, inaccurate_dol_jobs = helpers.geocode_and_split_by_accuracy(dol_jobs)
# merge all old and new data together
accurate_jobs, inaccurate_jobs = helpers.merge_all_data(accurate_dol_jobs, inaccurate_dol_jobs, accurate_old_jobs, inaccurate_old_jobs)

# push both dfs back to postgres
accurate_jobs.to_sql("job_central", engine, if_exists='replace', index=False, dtype=helpers.column_types)
inaccurate_jobs.to_sql("low_accuracies", engine, if_exists='replace', index=False, dtype=helpers.column_types)
