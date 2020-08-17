import os
import helpers
import pandas as pd
from sqlalchemy import create_engine
from geocodio import GeocodioClient
client = GeocodioClient("454565525ee5444fefef2572155e155e5248221")
engine = create_engine('postgres://txmzafvlwebrcr:df20d17265cf81634b9f689187248524a6fd0d56222985e2f422c71887ec6ec0@ec2-34-224-229-81.compute-1.amazonaws.com:5432/dbs39jork6o07d')

housing = pd.read_excel(os.path.join(os.getcwd(), '..', 'excel_files/housing_addendum.xlsx'))
helpers.fix_zip_code_columns(housing, ["PHYSICAL_LOCATION_POSTAL_CODE"])

# very similar code to populate_database.py
addresses = housing.apply(lambda job: helpers.create_address_from(job["PHYSICAL_LOCATION_ADDRESS1"], job["PHYSICAL_LOCATION_CITY"], job["PHYSICAL_LOCATION_STATE"], job["PHYSICAL_LOCATION_POSTAL_CODE"]), axis=1).tolist()
coordinates, accuracies, accuracy_types, failures = [], [], [], []
failures_count, count = 0, 0
for address in addresses:
    try:
        geocoded = client.geocode(address)
        accuracy_types.append(geocoded["results"][0]["accuracy_type"])
        coordinates.append(geocoded.coords)
        accuracies.append(geocoded.accuracy)
    except:
        coordinates.append(None)
        accuracies.append(None)
        accuracy_types.append(None)
        failures.append(address)
        failures_count += 1
    count += 1
    print(f"There have been {failures_count} failures out of {count} attempts")
print(len(coordinates), len(accuracies), len(housing), len(accuracy_types))
housing["coordinates"] = coordinates
housing["accuracy"] = accuracies
housing["accuracy type"] = accuracy_types
print(f"There were {failures_count} failures out of {count} attempts")

housing.to_sql("additional_housing", engine, if_exists='replace')
