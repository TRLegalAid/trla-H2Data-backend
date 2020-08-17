import os
import pandas as pd
import sqlalchemy
import helpers
from sqlalchemy import create_engine
from geocodio import GeocodioClient
client = GeocodioClient("454565525ee5444fefef2572155e155e5248221")
engine = create_engine('postgres://txmzafvlwebrcr:df20d17265cf81634b9f689187248524a6fd0d56222985e2f422c71887ec6ec0@ec2-34-224-229-81.compute-1.amazonaws.com:5432/dbs39jork6o07d')

# excel_sheet = input("Type the file path of an excel spreadsheet with your data - make sure this sheet this sheet is in the same folder as this script. \n")
# df = pd.read_excel(excel_sheet)
df = pd.read_excel(os.path.join(os.getcwd(), '..', 'excel_files/scraper_data.xlsx'))
df["fixed"], df["worksite_fixed_by"], df["housing_fixed_by"], df["notes"] = None, None, None, None
helpers.fix_zip_code_columns(df, ["Worksite address zip code", "Company zip code", "Place of Employment Info/Postal Code", "Housing Info/Postal Code"])
helpers.geocode_table(df, "worksite")
helpers.geocode_table(df, "housing")

# table_name = input('Type the name of the PostgreSQL table into which to put the data. If it already exists, it will be replaced, otherwise it will be created.')
# df.to_sql(table_name, engine, if_exists='replace')

# "Employer Telephone number": sqlalchemy.types.Integer, "Telephone number to apply": sqlalchemy.types.Integer
df.to_sql("todays_tests", engine, if_exists='replace', dtype={"Experience Required": sqlalchemy.types.Boolean, "Multiple Worksites": sqlalchemy.types.Boolean})
