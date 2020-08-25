import os
import pandas as pd
import sqlalchemy
import helpers
from sqlalchemy import create_engine
from geocodio import GeocodioClient
from dotenv import load_dotenv
database_connection_string, geocodio_api_key = helpers.get_secret_variables()
engine, client = create_engine(database_connection_string), GeocodioClient(geocodio_api_key)


df = pd.read_excel(os.path.join(os.getcwd(), '..', 'excel_files/scraper_data.xlsx'))
df = df.drop(columns=["Telephone number"])
df = helpers.rename_columns(df)
df = df.drop_duplicates(subset='CASE_NUMBER', keep="last")

df["fixed"], df["worksite_fixed_by"], df["housing_fixed_by"], df["notes"], df["table"] = None, None, None, None, "central"
helpers.fix_zip_code_columns(df, ["EMPLOYER_POSTAL_CODE", "WORKSITE_POSTAL_CODE",  "Place of Employment Info/Postal Code", "HOUSING_POSTAL_CODE"])
accurate_jobs, inaccurate_jobs = helpers.geocode_and_split_by_accuracy(df)


accurate_jobs.to_sql("job_central", engine, if_exists='replace', index=False, dtype={"Experience Required": sqlalchemy.types.Boolean, "Multiple Worksites": sqlalchemy.types.Boolean})
inaccurate_jobs.to_sql("low_accuracies", engine, if_exists='replace', index=False, dtype={"Experience Required": sqlalchemy.types.Boolean, "Multiple Worksites": sqlalchemy.types.Boolean})
