import pandas as pd
import numpy as np
import math
from sqlalchemy import create_engine
import psycopg2
from geocodio import GeocodioClient
import numpy as np
client = GeocodioClient("454565525ee5444fefef2572155e155e5248221")
engine = create_engine('postgres://txmzafvlwebrcr:df20d17265cf81634b9f689187248524a6fd0d56222985e2f422c71887ec6ec0@ec2-34-224-229-81.compute-1.amazonaws.com:5432/dbs39jork6o07d')


fixed = pd.read_sql_query('select * from "low_accuracies" where fixed=true', con=engine)

def mark_as_failed(i, worksite_or_housing, df):
    df.at[i, f"{worksite_or_housing}_fixed_by"] = "failed"
    df.at[i, "fixed"] = False

def fix_by_address(i, row, worksite_or_housing, df):
    try:
        geocoded = client.geocode(row[f"{worksite_or_housing} full address"])
        accuracy = geocoded.accuracy
        df.at[i, f"{worksite_or_housing} coordinates"] = geocoded.coords
        df.at[i, f"{worksite_or_housing} accuracy"] = accuracy
        if accuracy < 0.8:
            mark_as_failed(i, worksite_or_housing, df)

    except:
        mark_as_failed(i, worksite_or_housing, df)
        print("There was an error geocoding the address: " + row[f"{worksite_or_housing} full address"])

def fix_by_coords(i, worksite_or_housing, df):
    df.at[i, f"{worksite_or_housing} accuracy"] = 1

def assert_accuracy(i, row, worksite_or_housing, df):
    try:
        if row[f"{worksite_or_housing} accuracy"] < 0.8:
            mark_as_failed(i, worksite_or_housing, df)
    except:
        pass

def fix_row(i, row, worksite_or_housing, df):
    method = row[f"{worksite_or_housing}_fixed_by"]
    if method == "address":
        fix_by_address(i, row, worksite_or_housing, df)
    elif method == "coordinates":
        fix_by_coords(i, worksite_or_housing, df)
    elif method == "NA":
        assert_accuracy(i, row, worksite_or_housing, df)
    elif method == "impossible":
        pass
    else:
        mark_as_failed(i, worksite_or_housing, df)
        print("{worksite_or_housing}_fixed_by should be either `address`, `coordinates`, `impossible`, or `NA` - and it's case sensitive!")

def fix_rows(df):
    for i, row in df.iterrows():
        fix_row(i, row, "housing", df)
        fix_row(i, row, "worksite", df)

fix_rows(fixed)


# df = df.drop("fixed", axis=1)
# df = df.drop("worksite_fixed_by", axis=1)
# df = df.drop("housing_fixed_by", axis=1)



# get successfully fixed rows and add them to the big table
success_conditions = (fixed["worksite_fixed_by"] != "failed") & (fixed["housing_fixed_by"] != "failed")
successes = fixed[success_conditions]
successes.to_sql('todays_tests', engine, if_exists='append', index=False)



# get failed fixes dataframe and not yet fixed dataframe, put them both into low_accuracies table
failure_conditions = (fixed["worksite_fixed_by"] == "failed") | (fixed["housing_fixed_by"] == "failed")
failures = fixed[failure_conditions]
not_fixed = pd.read_sql_query('select * from "low_accuracies" where fixed=false', con=engine)
failures_and_not_fixed = failures.append(not_fixed, ignore_index=True)
# not_fixed.to_sql('low_accuracies', engine, if_exists='replace', index=False)
# failures.to_sql('low_accuracies', engine, if_exists='append', index=False)
failures_and_not_fixed.to_sql('low_accuracies', engine, if_exists='replace', index=False)
