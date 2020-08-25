import os
import pandas as pd
import sqlalchemy
import helpers
from sqlalchemy import create_engine
from geocodio import GeocodioClient
client = GeocodioClient("454565525ee5444fefef2572155e155e5248221")
engine = create_engine('postgres://txmzafvlwebrcr:df20d17265cf81634b9f689187248524a6fd0d56222985e2f422c71887ec6ec0@ec2-34-224-229-81.compute-1.amazonaws.com:5432/dbs39jork6o07d')

df = pd.read_excel(os.path.join(os.getcwd(), '..', 'excel_files/scraper_data.xlsx'))
df = df.drop(columns=["Telephone number"])
df = helpers.rename_columns(df)

df["fixed"], df["worksite_fixed_by"], df["housing_fixed_by"], df["notes"], df["table"] = None, None, None, None, "central"
helpers.fix_zip_code_columns(df, ["EMPLOYER_POSTAL_CODE", "WORKSITE_POSTAL_CODE",  "Place of Employment Info/Postal Code", "HOUSING_POSTAL_CODE"])
accurate_jobs, inaccurate_jobs = helpers.geocode_and_split_by_accuracy(df)


accurate_jobs.to_sql("job_central", engine, if_exists='replace', index=False, dtype={"Experience Required": sqlalchemy.types.Boolean, "Multiple Worksites": sqlalchemy.types.Boolean})
inaccurate_jobs.to_sql("low_accuracies", engine, if_exists='replace', index=False, dtype={"Experience Required": sqlalchemy.types.Boolean, "Multiple Worksites": sqlalchemy.types.Boolean})
