import os
import helpers
import pandas as pd
from sqlalchemy import create_engine
from geocodio import GeocodioClient
client = GeocodioClient("454565525ee5444fefef2572155e155e5248221")
engine = create_engine('postgres://txmzafvlwebrcr:df20d17265cf81634b9f689187248524a6fd0d56222985e2f422c71887ec6ec0@ec2-34-224-229-81.compute-1.amazonaws.com:5432/dbs39jork6o07d')

housing = pd.read_excel(os.path.join(os.getcwd(), '..', 'excel_files/housing_addendum.xlsx'))
helpers.fix_zip_code_columns(housing, ["PHYSICAL_LOCATION_POSTAL_CODE"])
housing["table"], housing["source"] = "housing", "DOL"

helpers.geocode_table(housing, "housing addendum")
accurate_housing, inaccurate_housing = helpers.split_df_by_accuracies(housing)
accurate_housing.to_sql("additional_housing", engine, if_exists='replace')

with engine.connect() as connection:
    low_accuracies_columns = connection.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'low_accuracies'")

low_accuracies_columns = [column[0] for column in list(low_accuracies_columns)]
columns_only_in_addendum = [column for column in housing.columns if column not in low_accuracies_columns]
query_to_add_columns = ""
columns_added = 0
for column in columns_only_in_addendum:
    if columns_added == 0:
        query_to_add_columns += f'ALTER TABLE low_accuracies ADD COLUMN "{column}" text'
    else:
        query_to_add_columns += f', ADD COLUMN "{column}" text'
    columns_added += 1

# it'll be an empty string if there are no columns in the housing addendum that aren't in postgres yet
if query_to_add_columns != "":
    with engine.connect() as connection:
        connection.execute(query_to_add_columns)
inaccurate_housing.to_sql("low_accuracies", engine, if_exists='append', index=False)
