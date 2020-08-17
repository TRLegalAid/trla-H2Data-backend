import os
import helpers
import pandas as pd
from sqlalchemy import create_engine
from geocodio import GeocodioClient
client = GeocodioClient("454565525ee5444fefef2572155e155e5248221")
engine = create_engine('postgres://txmzafvlwebrcr:df20d17265cf81634b9f689187248524a6fd0d56222985e2f422c71887ec6ec0@ec2-34-224-229-81.compute-1.amazonaws.com:5432/dbs39jork6o07d')

# get dol data and data that's already in postgres, divide postgres data into h2a and h2b
dol_jobs = pd.read_excel(os.path.join(os.getcwd(), '..', 'excel_files/dol_data.xlsx'))
helpers.fix_zip_code_columns(dol_jobs, ["HOUSING_POSTAL_CODE", "EMPLOYER_POC_POSTAL_CODE", "EMPLOYER_POSTAL_CODE", "WORKSITE_POSTAL_CODE", "ATTORNEY_AGENT_POSTAL_CODE"])
old_jobs = pd.read_sql("todays_tests", con=engine)
old_h2a = old_jobs[old_jobs["Visa type"] == "H-2A"]
old_h2b = old_jobs[old_jobs["Visa type"] == "H-2B"]

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
    column_mappings_dict[mapped_dol_cols[i]] = mapped_old_cols[i]

# change columns names in dol to match those in h2a, where applicable, add necessary columns to dol data, convert "Multiple Worksites" column to boolean
dol_jobs = dol_jobs.rename(columns=column_mappings_dict)
dol_jobs["Source"] = "DOL"
dol_jobs["Job Order Link"] = dol_jobs.apply(lambda job: "https://seasonaljobs.dol.gov/job-order/" + job["ETA case number"], axis=1)


def h2a_or_h2b(job):
    if job["ETA case number"][2] == "3":
        return "H-2A"
    elif job["ETA case number"][2] == "4":
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

# geocode dol data
helpers.geocode_table(dol_jobs, "worksite")
helpers.geocode_table(dol_jobs, "housing")

# merge dol with h2a: if a case number exists in both tables, keep all the columns, but for the columns with the same names, use the value in dol, and change the "Source" value for that row to "DOL". if a case number only exits in one table, just add the row as is
dol_columns, h2a_columns = dol_jobs.columns.tolist(), old_h2a.columns.tolist()
cols_only_in_h2a = [column for column in h2a_columns if column not in dol_columns and column != "index"]
for column in cols_only_in_h2a:
    dol_jobs[column] = None
h2a_case_numbers = old_h2a["ETA case number"].tolist()
for i, row in dol_jobs.iterrows():
    if row["ETA case number"] in h2a_case_numbers:
        # add the columns that are in the old_h2a but not in dol_jobs to this row
        for column in cols_only_in_h2a:
            dol_jobs.at[i, column] = old_h2a[old_h2a["ETA case number"] == row["ETA case number"]][column].tolist()[0]

# get dataframe of postings that are only in the scraper data and append that to the dol dataframe
dol_case_numbers = dol_jobs["ETA case number"].tolist()
boolean_series = old_h2a.apply(lambda job: job["ETA case number"] not in dol_case_numbers, axis=1)
only_in_h2a = old_h2a[boolean_series]
dol_jobs = dol_jobs.append(only_in_h2a, sort=True, ignore_index=True)

# append h2b to dataframe
old_h2b = dol_jobs.append(old_h2b, sort=True, ignore_index=True)

dol_jobs.to_sql("todays_tests", engine, if_exists='replace')
