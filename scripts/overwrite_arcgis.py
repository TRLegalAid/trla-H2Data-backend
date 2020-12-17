from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection
from colorama import Fore, Style
from helpers import myprint, get_database_engine, get_value, print_red_and_email
import os
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

ARCGIS_USERNAME, ARCGIS_PASSWORD = os.getenv("ARCGIS_USERNAME"), os.getenv("ARCGIS_PASSWORD")
engine = get_database_engine(force_cloud=True)

# overwrites feature named old_feature_name with data from new_df using the arcGIS account specified by username/password
def overwrite_feature(username, password, new_df, old_feature_name):
    gis = GIS(url='https://www.arcgis.com', username=username, password=password)
    # print("Logged in as " + str(gis.properties.user.username))

    csv_file_name = f"{old_feature_name}.csv"
    new_df.to_csv(csv_file_name, index=False)

    # get first search resul
    old_jobs_item = gis.content.search(f"title: {old_feature_name}", 'Feature Layer')[0]
    old_feature_layer = FeatureLayerCollection.fromitem(old_jobs_item)

    myprint(f"Overwriting feature layer.... there will now be {len(new_df)} features.")
    old_feature_layer.manager.overwrite(csv_file_name)
    myprint('Done overwriting feature layer.')

    os.remove(csv_file_name)


# overwrites our feautre layer with all h2a and forestry h2b jobs from job_central and all additional_housing rows
def overwrite_our_feature():

    # get all accurate h2a jobs that are in one of our states and have housing coordinates
    h2a_df = pd.read_sql("""SELECT * FROM job_central WHERE
                        "Visa type" = 'H-2A' AND
                        LOWER("WORKSITE_STATE") IN
                        ('texas', 'tx', 'kentucky', 'ky', 'tennessee', 'tn', 'arkansas', 'ar', 'louisiana', 'la', 'mississippi', 'ms', 'alabama', 'al') AND
                        housing_lat IS NOT NUll AND housing_long IS NOT NULL""",
                        con=engine)

    # get all h2a jobs from job_central that are in one of our states and do not have housing coordinates
    h2a_no_housing_df = pd.read_sql("""SELECT * FROM job_central WHERE
                                       "Visa type" = 'H-2A' AND
                                       LOWER("WORKSITE_STATE") IN
                                       ('texas', 'tx', 'kentucky', 'ky', 'tennessee', 'tn', 'arkansas', 'ar', 'louisiana', 'la', 'mississippi', 'ms', 'alabama', 'al') AND
                                       (housing_lat IS NUll OR housing_long IS NULL)""", con=engine)

    # Lizzie's request - can't remember the purpose
    h2a_df["TOTAL_OCCUPANCY"].fillna(600, inplace=True)
    h2a_no_housing_df["TOTAL_OCCUPANCY"].fillna(600, inplace=True)

    # get all forerstry h2b jobs from job_central that are in one of our states
    forestry_h2b_in_our_states_df = pd.read_sql("""SELECT * FROM job_central WHERE
                                                   "Visa type" = 'H-2B' AND
                                                   "SOC_CODE" IN ('45-4011.00', '45-4011') AND
                                                   LOWER("WORKSITE_STATE") IN
                                                   ('texas', 'tx', 'kentucky', 'ky', 'tennessee', 'tn', 'arkansas', 'ar', 'louisiana', 'la', 'mississippi', 'ms', 'alabama', 'al')
                                                    """, con=engine)

    # set housing coordinates of h2b jobs and h2a jobs without housing to their worksite coordinates so that arecGIS will map them
    forestry_h2b_in_our_states_df["housing_lat"] = forestry_h2b_in_our_states_df.apply(lambda job: job["worksite_lat"], axis=1)
    forestry_h2b_in_our_states_df["housing_long"] = forestry_h2b_in_our_states_df.apply(lambda job: job["worksite_long"], axis=1)
    h2a_no_housing_df["housing_lat"] = h2a_no_housing_df.apply(lambda job: job["worksite_lat"], axis=1)
    h2a_no_housing_df["housing_long"] = h2a_no_housing_df.apply(lambda job: job["worksite_long"], axis=1)

    # combine h2a and forestry data
    h2a_and_h2b_df = h2a_df.append(forestry_h2b_in_our_states_df)
    h2a_housing_and_no_housing_and_h2b_df = h2a_and_h2b_df.append(h2a_no_housing_df)

    # get all additional housing rows that are in one of our states and that have a matching case number in job_central
    additional_housing_df = pd.read_sql("""SELECT * FROM additional_housing WHERE
                                           "CASE_NUMBER" IN
                                                (SELECT "CASE_NUMBER" FROM job_central WHERE
                                                "Visa type" = 'H-2A' AND
                                                LOWER("WORKSITE_STATE") IN
                                                ('texas', 'tx', 'kentucky', 'ky', 'tennessee', 'tn', 'arkansas', 'ar', 'louisiana', 'la', 'mississippi', 'ms', 'alabama', 'al'))
                                                 """, con=engine)

    myprint(f"There will be {len(h2a_df)} normal H2A jobs in the feature.")
    myprint(f"There will be {len(h2a_no_housing_df)} H2A jobs mapped using their worksites in the feature.")
    myprint(f"There will be {len(forestry_h2b_in_our_states_df)} forestry H2B jobs in the feature.")
    myprint(f"There will be {len(additional_housing_df)} additional housing rows in the feature.")

    # get columns that are in the h2a data but not the additional housing data and add each one to the additional housing datafrane
    cols_only_in_h2a = set(h2a_df.columns) - set(additional_housing_df.columns)
    for column in cols_only_in_h2a:
        additional_housing_df[column] = None

    # for each additional housing row, find its matching row in job_central and insert the data about that case number that is in job_central but not the additional_housing row
    for i, row in additional_housing_df.iterrows():
        case_number = row["CASE_NUMBER"]
        job_in_h2a = h2a_df[h2a_df["CASE_NUMBER"] == case_number]

        if len(job_in_h2a) == 1:
            for column in cols_only_in_h2a:
                additional_housing_df.at[i, column] = get_value(job_in_h2a, column)
        else:
            print_red_and_email(f"{case_number} is in additional_housing, so I looked for it in job_central, and found a number of matching rows not equal to 1.", "Overwriting ArcGIS Layer")

    # append completed additional_housing df to the h2a and forestry data
    full_layer = h2a_housing_and_no_housing_and_h2b_df.append(additional_housing_df)

    overwrite_feature(ARCGIS_USERNAME, ARCGIS_PASSWORD, full_layer, 'H2Data')

if __name__ == "__main__":
   overwrite_our_feature()
