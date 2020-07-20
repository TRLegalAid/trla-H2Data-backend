import pandas as pd
import numpy as np
import math
from sqlalchemy import create_engine
import psycopg2
from geocodio import GeocodioClient
import numpy as np
client = GeocodioClient("454565525ee5444fefef2572155e155e5248221")
engine = create_engine('postgres://txmzafvlwebrcr:df20d17265cf81634b9f689187248524a6fd0d56222985e2f422c71887ec6ec0@ec2-34-224-229-81.compute-1.amazonaws.com:5432/dbs39jork6o07d')


df = pd.read_sql_query('select * from "low_accuracies" where fixed=true', con=engine)

def geocode_table(worskite_or_housing):
    addresses = df[f"{worskite_or_housing} full address"].tolist()
    coordinates, accuracies, failures = [], [], []
    failures_count, count = 0, 0
    for address in addresses:
        try:
            geocoded = client.geocode(address)
            coordinates.append(geocoded.coords)
            print(geocoded.coords)
            accuracies.append(geocoded.accuracy)
        except:
            coordinates.append(None)
            accuracies.append(None)
            failures.append(address)
            failures_count += 1
        count += 1
        print(f"There have been {failures_count} failures out of {count} attempts")
    df[f"{worskite_or_housing} coordinates"] = coordinates
    df[f"{worskite_or_housing} accuracy"] = accuracies
    print(f"There were {failures_count} failures out of {count} attempts")

geocode_table("worksite")
geocode_table("housing")

df = df.drop("fixed", axis=1)
df.to_sql('todays_tests', engine, if_exists='append', index=False)

df = pd.read_sql_query('select * from "low_accuracies" where fixed=false', con=engine)
df.to_sql('low_accuracies', engine, if_exists='replace', index=False)
