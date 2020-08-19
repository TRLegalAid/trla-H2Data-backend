import os
import pandas as pd
import sqlalchemy
import helpers
from sqlalchemy import create_engine
from geocodio import GeocodioClient
client = GeocodioClient("454565525ee5444fefef2572155e155e5248221")
engine = create_engine('postgres://txmzafvlwebrcr:df20d17265cf81634b9f689187248524a6fd0d56222985e2f422c71887ec6ec0@ec2-34-224-229-81.compute-1.amazonaws.com:5432/dbs39jork6o07d')

df = pd.read_excel(os.path.join(os.getcwd(), '..', 'excel_files/scraper_data.xlsx'))
df["fixed"], df["worksite_fixed_by"], df["housing_fixed_by"], df["notes"] = None, None, None, None
helpers.fix_zip_code_columns(df, ["Worksite address zip code", "Company zip code", "Place of Employment Info/Postal Code", "Housing Info/Postal Code"])
helpers.geocode_table(df, "worksite")
helpers.geocode_table(df, "housing")

list_of_jobs = df.to_dict(orient='records')
accurate_jobs_list, inaccurate_jobs_list = helpers.check_accuracies(list_of_jobs)
accurate_jobs, inaccurate_jobs = pd.DataFrame(accurate_jobs_list), pd.DataFrame(inaccurate_jobs_list)

accurate_jobs.to_sql("todays_tests", engine, if_exists='replace', dtype={"Experience Required": sqlalchemy.types.Boolean, "Multiple Worksites": sqlalchemy.types.Boolean})
inaccurate_jobs.to_sql("low_accuracies", engine, if_exists='replace', dtype={"Experience Required": sqlalchemy.types.Boolean, "Multiple Worksites": sqlalchemy.types.Boolean})
