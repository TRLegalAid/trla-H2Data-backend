from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection
from colorama import Fore, Style
from helpers import myprint, get_database_engine, get_value, print_red_and_email
import os
import pandas as pd
from sqlalchemy import create_engine
ARCGIS_USERNAME, ARCGIS_PASSWORD = os.getenv("ARCGIS_USERNAME"), os.getenv("ARCGIS_PASSWORD")
engine = get_database_engine(force_cloud=True)

def overwrite_feature(username, password, new_df, old_feature_name):
    gis = GIS(url='https://www.arcgis.com', username=username, password=password)
    # print("Logged in as " + str(gis.properties.user.username))

    csv_file_name = f"{old_feature_name}.csv"
    new_df.to_csv(csv_file_name, index=False)

    old_jobs_item = gis.content.search(f"title: {old_feature_name}", 'Feature Layer')[0]
    old_feature_layer = FeatureLayerCollection.fromitem(old_jobs_item)

    myprint(f"Overwriting feature layer.... there will now be {len(new_df)} features.")
    old_feature_layer.manager.overwrite(csv_file_name)
    myprint('Done overwriting feature layer.')

    os.remove(csv_file_name)

def overwrite_our_feature():

    h2a_df = pd.read_sql("""SELECT * FROM job_central WHERE "Visa type" = 'H-2A' AND "WORKSITE_STATE" IN
                        ('TEXAS', 'KENTUCKY', 'TENNESSEE', 'ARKANSAS', 'LOUISIANA', 'MISSISSIPPI', 'ALABAMA')
                        AND housing_lat IS NOT NUll AND housing_long IS NOT NULL""", con=engine)

    forestry_h2b_in_our_states_df = pd.read_sql("""SELECT * FROM job_central WHERE "Visa type" = 'H-2B' AND "SOC_CODE" IN ('45-4011.00', '45-4011') AND
                                                    "WORKSITE_STATE" IN ('TEXAS', 'KENTUCKY', 'TENNESSEE', 'ARKANSAS', 'LOUISIANA', 'MISSISSIPPI', 'ALABAMA')
                                                    """, con=engine)

    h2a_no_housing_df = pd.read_sql("""SELECT * FROM job_central WHERE "Visa type" = 'H-2A' AND "WORKSITE_STATE" IN
                        ('TEXAS', 'KENTUCKY', 'TENNESSEE', 'ARKANSAS', 'LOUISIANA', 'MISSISSIPPI', 'ALABAMA')
                        AND (housing_lat IS NUll OR housing_long IS NULL)""", con=engine)

    myprint(f"There will be {len(h2a_df)} normal H2A jobs in the feature.")
    myprint(f"There will be {len(h2a_no_housing_df)} H2A jobs mapped using their worksites in the feature.")
    myprint(f"There will be {len(forestry_h2b_in_our_states_df)} forestry H2B jobs in the feature.")

    def get_worksite_lat(job):
        return job["worksite_lat"]

    def get_worksite_long(job):
        return job["worksite_long"]

    forestry_h2b_in_our_states_df["housing_lat"] = forestry_h2b_in_our_states_df.apply(lambda job: get_worksite_lat(job), axis=1)
    forestry_h2b_in_our_states_df["housing_long"] = forestry_h2b_in_our_states_df.apply(lambda job: get_worksite_long(job), axis=1)
    h2a_and_h2b_df = h2a_df.append(forestry_h2b_in_our_states_df)

    h2a_no_housing_df["housing_lat"] = h2a_no_housing_df.apply(lambda job: get_worksite_lat(job), axis=1)
    h2a_no_housing_df["housing_long"] = h2a_no_housing_df.apply(lambda job: get_worksite_long(job), axis=1)
    h2a_housing_and_no_housing_and_h2b_df = h2a_and_h2b_df.append(h2a_no_housing_df)

    additional_housing_df = pd.read_sql("""SELECT * FROM additional_housing WHERE "CASE_NUMBER" IN (select "CASE_NUMBER" FROM job_central WHERE
                                        "Visa type" = 'H-2A' AND "WORKSITE_STATE" IN ('TEXAS', 'KENTUCKY', 'TENNESSEE', 'ARKANSAS', 'LOUISIANA', 'MISSISSIPPI', 'ALABAMA'))
                                         """, con=engine)

    myprint(f"There will be {len(additional_housing_df)} additional housing rows in the feature.")

    h2a_columns = set(h2a_df.columns)
    additional_housing_columns = set(additional_housing_df.columns)
    cols_only_in_h2a = h2a_columns - additional_housing_columns

    for column in cols_only_in_h2a:
        additional_housing_df[column] = None

    for i, row in additional_housing_df.iterrows():
        case_number = row["CASE_NUMBER"]
        job_in_h2a = h2a_df[h2a_df["CASE_NUMBER"] == case_number]

        if len(job_in_h2a) == 1:
            for column in cols_only_in_h2a:
                additional_housing_df.at[i, column] = get_value(job_in_h2a, column)
        elif len(job_in_h2a) > 1:
            print_red_and_email(f"{case_number} is in additional_housing, so I looked for it in job_central, and found {len(job_in_h2a)} rows with that case number when I should have only found 1 such row!", "Found Duplicate Case Number in job_central while Overwriting ArcGIS Layer")

    full_layer = h2a_housing_and_no_housing_and_h2b_df.append(additional_housing_df)
    # full_layer.to_csv("H2Data.csv")
    # exit()

    overwrite_feature(ARCGIS_USERNAME, ARCGIS_PASSWORD, full_layer, 'H2Data')

if __name__ == "__main__":
   overwrite_our_feature()

# code to create a new layer
# gis = GIS(url='https://www.arcgis.com', username=ARCGIS_USERNAME, password=ARCGIS_PASSWORD)
# job_central_df = pd.read_sql('job_central', con=engine).head(10)
# file_name = "h2a_h2b_job_postings.csv"
# job_central_df.to_csv(file_name)
# properties = {'title': 'H-2A and H-2B Job Postings', 'description': 'Datatset of H-2A and H-2B job postings. Updated daily','tags': 'data', 'type': 'CSV'}
# to_publish = gis.content.add(properties, data=file_name)
# item = to_publish.publish()
# print("layer published.")
