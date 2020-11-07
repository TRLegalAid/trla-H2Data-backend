from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection
from colorama import Fore, Style
from helpers import myprint, get_database_engine, get_value, print_red_and_email
import os
import pandas as pd
from sqlalchemy import create_engine
if os.getenv("LOCAL_DEV") == "true":
    from dotenv import load_dotenv
    load_dotenv()
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

    h2a_df = pd.read_sql("""SELECT
                            "housing_lat", "housing_long", "HOUSING_ADDRESS_LOCATION", "HOUSING_CITY", "HOUSING_POSTAL_CODE",
                           "HOUSING_STATE", "CASE_NUMBER", "Visa type", "TOTAL_WORKERS_H-2A_REQUESTED", "EMPLOYER_NAME", "Job duties",
                           "JOB_TITLE", "TOTAL_OCCUPANCY", "table", "RECEIVED_DATE", "TOTAL_WORKERS_NEEDED",
                           "TYPE_OF_HOUSING", "ADDITIONAL_JOB_REQUIREMENTS", "WORKSITE_ADDRESS", "WORKSITE_ADDRESS2", "WORKSITE_CITY",
                           "WORKSITE_COUNTY", "WORKSITE_POSTAL_CODE", "WORKSITE_STATE", "SOC_CODE", "SOC_TITLE", "EMPLOYMENT_BEGIN_DATE",
                           "EMPLOYMENT_END_DATE", "status", "housing accuracy", "HOUSING_COUNTY", "worksite_long", "worksite_lat",
                           "EMPLOYER_ADDRESS1", "EMPLOYER_ADDRESS2", "EMPLOYER_ADDRESS_1", "EMPLOYER_ADDRESS_2", "EMPLOYER_APPENDIX_B_ATTACHED",
                           "EMPLOYER_CITY", "EMPLOYER_COUNTRY", "EMPLOYER_MSPA_ATTACHED", "EMPLOYER_PHONE",
                           "EMPLOYER_PHONE_EXT", "EMPLOYER_POC_ADDRESS1", "EMPLOYER_POC_ADDRESS2", "EMPLOYER_POC_CITY", "EMPLOYER_POC_COUNTRY",
                           "EMPLOYER_POC_EMAIL", "EMPLOYER_POC_FIRST_NAME", "EMPLOYER_POC_JOB_TITLE", "EMPLOYER_POC_LAST_NAME",
                           "EMPLOYER_POC_MIDDLE_NAME", "EMPLOYER_POC_PHONE", "EMPLOYER_POC_PHONE_EXT", "EMPLOYER_POC_POSTAL_CODE",
                           "EMPLOYER_POC_PROVINCE", "EMPLOYER_POC_STATE", "EMPLOYER_POSTAL_CODE", "EMPLOYER_PROVINCE", "EMPLOYER_STATE",
                           "EMAIL_TO_APPLY", "ATTORNEY_AGENT_ADDRESS_1", "ATTORNEY_AGENT_ADDRESS_2", "ATTORNEY_AGENT_CITY", "ATTORNEY_AGENT_COUNTRY",
                           "ATTORNEY_AGENT_EMAIL", "ATTORNEY_AGENT_EMAIL_ADDRESS", "ATTORNEY_AGENT_FIRST_NAME", "ATTORNEY_AGENT_LAST_NAME",
                           "ATTORNEY_AGENT_MIDDLE_NAME", "ATTORNEY_AGENT_PHONE", "ATTORNEY_AGENT_PHONE_EXT", "ATTORNEY_AGENT_POSTAL_CODE",
                           "ATTORNEY_AGENT_PROVINCE", "ATTORNEY_AGENT_STATE"
                            FROM job_central WHERE "Visa type" = 'H-2A' AND
                        	lower("WORKSITE_STATE") IN ('texas', 'kentucky', 'tennessee', 'arkansas', 'louisiana', 'mississippi', 'alabama') AND
                            (lower("WORKSITE_COUNTY") in ('dallam', 'potter', 'moore', 'hartley') OR
                            lower("HOUSING_COUNTY") IN ('dallam', 'potter', 'moore', 'hartley')) AND
                            (("status" = 'active') OR (EXTRACT(MONTH FROM "EMPLOYMENT_BEGIN_DATE") = 11 AND
                             EXTRACT(YEAR FROM "EMPLOYMENT_BEGIN_DATE") = 2020))
                            """, con=engine)




    forestry_h2b_in_our_states_df = pd.read_sql("""SELECT
                                                   "housing_lat", "housing_long", "HOUSING_ADDRESS_LOCATION", "HOUSING_CITY", "HOUSING_POSTAL_CODE",
                                                   "HOUSING_STATE", "CASE_NUMBER", "Visa type", "TOTAL_WORKERS_H-2A_REQUESTED", "EMPLOYER_NAME", "Job duties",
                                                   "JOB_TITLE", "TOTAL_OCCUPANCY", "table", "RECEIVED_DATE", "TOTAL_WORKERS_NEEDED",
                                                   "TYPE_OF_HOUSING", "ADDITIONAL_JOB_REQUIREMENTS", "WORKSITE_ADDRESS", "WORKSITE_ADDRESS2", "WORKSITE_CITY",
                                                   "WORKSITE_COUNTY", "WORKSITE_POSTAL_CODE", "WORKSITE_STATE", "SOC_CODE", "SOC_TITLE", "EMPLOYMENT_BEGIN_DATE",
                                                   "EMPLOYMENT_END_DATE", "status", "housing accuracy", "HOUSING_COUNTY", "worksite_long", "worksite_lat",
                                                   "EMPLOYER_ADDRESS1", "EMPLOYER_ADDRESS2", "EMPLOYER_ADDRESS_1", "EMPLOYER_ADDRESS_2", "EMPLOYER_APPENDIX_B_ATTACHED",
                                                   "EMPLOYER_CITY", "EMPLOYER_COUNTRY", "EMPLOYER_MSPA_ATTACHED", "EMPLOYER_PHONE",
                                                   "EMPLOYER_PHONE_EXT", "EMPLOYER_POC_ADDRESS1", "EMPLOYER_POC_ADDRESS2", "EMPLOYER_POC_CITY", "EMPLOYER_POC_COUNTRY",
                                                   "EMPLOYER_POC_EMAIL", "EMPLOYER_POC_FIRST_NAME", "EMPLOYER_POC_JOB_TITLE", "EMPLOYER_POC_LAST_NAME",
                                                   "EMPLOYER_POC_MIDDLE_NAME", "EMPLOYER_POC_PHONE", "EMPLOYER_POC_PHONE_EXT", "EMPLOYER_POC_POSTAL_CODE",
                                                   "EMPLOYER_POC_PROVINCE", "EMPLOYER_POC_STATE", "EMPLOYER_POSTAL_CODE", "EMPLOYER_PROVINCE", "EMPLOYER_STATE",
                                                   "EMAIL_TO_APPLY", "ATTORNEY_AGENT_ADDRESS_1", "ATTORNEY_AGENT_ADDRESS_2", "ATTORNEY_AGENT_CITY", "ATTORNEY_AGENT_COUNTRY",
                                                   "ATTORNEY_AGENT_EMAIL", "ATTORNEY_AGENT_EMAIL_ADDRESS", "ATTORNEY_AGENT_FIRST_NAME", "ATTORNEY_AGENT_LAST_NAME",
                                                   "ATTORNEY_AGENT_MIDDLE_NAME", "ATTORNEY_AGENT_PHONE", "ATTORNEY_AGENT_PHONE_EXT", "ATTORNEY_AGENT_POSTAL_CODE",
                                                   "ATTORNEY_AGENT_PROVINCE", "ATTORNEY_AGENT_STATE"
                                                    FROM job_central WHERE "Visa type" = 'H-2B' AND
                                                    "SOC_CODE" IN ('45-4011.00', '45-4011') AND
                                                    lower("WORKSITE_STATE") IN ('TEXAS', 'KENTUCKY', 'TENNESSEE', 'ARKANSAS', 'LOUISIANA', 'MISSISSIPPI', 'ALABAMA') AND
                                                    (lower("WORKSITE_COUNTY") in ('dallam', 'potter', 'moore', 'hartley') OR
                                                    lower("HOUSING_COUNTY") IN ('dallam', 'potter', 'moore', 'hartley')) AND
                                                    (("status" = 'active') OR (EXTRACT(MONTH FROM "EMPLOYMENT_BEGIN_DATE") = 11 AND
                                                     EXTRACT(YEAR FROM "EMPLOYMENT_BEGIN_DATE") = 2020))
                                                    """, con=engine)


    myprint(f"There will be {len(h2a_df)} H2A jobs in the feature.")
    myprint(f"There will be {len(forestry_h2b_in_our_states_df)} forestry H2B jobs in the feature.")

    def get_worksite_lat(job):
        return job["worksite_lat"]

    def get_worksite_long(job):
        return job["worksite_long"]

    forestry_h2b_in_our_states_df["housing_lat"] = forestry_h2b_in_our_states_df.apply(lambda job: get_worksite_lat(job), axis=1)
    forestry_h2b_in_our_states_df["housing_long"] = forestry_h2b_in_our_states_df.apply(lambda job: get_worksite_long(job), axis=1)
    h2a_and_h2b_df = h2a_df.append(forestry_h2b_in_our_states_df)

    additional_housing_df_accurate = pd.read_sql("""SELECT
                                        "CASE_NUMBER", "TYPE_OF_HOUSING", "PHYSICAL_LOCATION_ADDRESS1", "PHYSICAL_LOCATION_CITY",
                                        "PHYSICAL_LOCATION_STATE", "PHYSICAL_LOCATION_POSTAL_CODE", "PHYSICAL_LOCATION_COUNTY",
                                        "TOTAL_OCCUPANCY", "table", "housing accuracy", "housing_long", "housing_lat"
                                         FROM additional_housing WHERE "CASE_NUMBER" IN
                                          (select "CASE_NUMBER"
                                          FROM job_central WHERE "Visa type" = 'H-2A' AND
                                        	lower("WORKSITE_STATE") IN ('texas', 'kentucky', 'tennessee', 'arkansas', 'louisiana', 'mississippi', 'alabama') AND
                                          (lower("WORKSITE_COUNTY") in ('dallam', 'potter', 'moore', 'hartley') OR
                                          lower("HOUSING_COUNTY") IN ('dallam', 'potter', 'moore', 'hartley')) AND
                                          (("status" = 'active') OR (EXTRACT(MONTH FROM "EMPLOYMENT_BEGIN_DATE") = 11 AND
                                           EXTRACT(YEAR FROM "EMPLOYMENT_BEGIN_DATE") = 2020)))
                                          """, con=engine)

    additional_housing_df_inaccurate = pd.read_sql("""SELECT
                                        "CASE_NUMBER", "TYPE_OF_HOUSING", "PHYSICAL_LOCATION_ADDRESS1", "PHYSICAL_LOCATION_CITY",
                                        "PHYSICAL_LOCATION_STATE", "PHYSICAL_LOCATION_POSTAL_CODE", "PHYSICAL_LOCATION_COUNTY",
                                        "TOTAL_OCCUPANCY", "table", "housing accuracy", "housing_long", "housing_lat"
                                         FROM low_accuracies WHERE ("table" = 'dol_h') AND
                                         "CASE_NUMBER" IN
                                          (select "CASE_NUMBER"
                                          FROM job_central WHERE "Visa type" = 'H-2A' AND
                                        	lower("WORKSITE_STATE") IN ('texas', 'kentucky', 'tennessee', 'arkansas', 'louisiana', 'mississippi', 'alabama') AND
                                          (lower("WORKSITE_COUNTY") in ('dallam', 'potter', 'moore', 'hartley') OR
                                          lower("HOUSING_COUNTY") IN ('dallam', 'potter', 'moore', 'hartley')) AND
                                          (("status" = 'active') OR (EXTRACT(MONTH FROM "EMPLOYMENT_BEGIN_DATE") = 11 AND
                                           EXTRACT(YEAR FROM "EMPLOYMENT_BEGIN_DATE") = 2020)))
                                          """, con=engine)

    additional_housing_df = additional_housing_df_accurate.append(additional_housing_df_inaccurate)
    myprint(f"There will be {len(additional_housing_df)} additional housing rows in the feature. ({len(additional_housing_df_accurate)} accurate and {len(additional_housing_df_inaccurate)} inaccurate)")

    h2a_columns = set(h2a_df.columns)
    additional_housing_columns = set(additional_housing_df.columns)
    cols_only_in_h2a = h2a_columns - additional_housing_columns
    address_columns_mappings = {"HOUSING_ADDRESS_LOCATION": "PHYSICAL_LOCATION_ADDRESS1", "HOUSING_CITY": "PHYSICAL_LOCATION_CITY",
                                "HOUSING_STATE": "PHYSICAL_LOCATION_STATE", "HOUSING_POSTAL_CODE": "PHYSICAL_LOCATION_POSTAL_CODE",
                                "HOUSING_COUNTY": "PHYSICAL_LOCATION_COUNTY"}

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

        for h2a_address_column in address_columns_mappings:
            additional_housing_df.at[i, h2a_address_column] = row[address_columns_mappings[h2a_address_column]]

    additional_housing_df = additional_housing_df.drop(columns=address_columns_mappings.values())

    columns_to_keep = ["housing_lat", "housing_long", "HOUSING_ADDRESS_LOCATION", "HOUSING_CITY", "HOUSING_POSTAL_CODE",
                       "HOUSING_STATE", "CASE_NUMBER", "Visa type", "TOTAL_WORKERS_H-2A_REQUESTED", "EMPLOYER_NAME", "Job duties",
                       "JOB_TITLE", "TOTAL_OCCUPANCY", "table", "RECEIVED_DATE", "TOTAL_WORKERS_NEEDED",
                       "TYPE_OF_HOUSING", "ADDITIONAL_JOB_REQUIREMENTS", "WORKSITE_ADDRESS", "WORKSITE_ADDRESS2", "WORKSITE_CITY",
                       "WORKSITE_COUNTY", "WORKSITE_POSTAL_CODE", "WORKSITE_STATE", "SOC_CODE", "SOC_TITLE", "EMPLOYMENT_BEGIN_DATE",
                       "EMPLOYMENT_END_DATE", "status", "housing accuracy", "HOUSING_COUNTY", "worksite_long", "worksite_lat",
                       "EMPLOYER_ADDRESS1", "EMPLOYER_ADDRESS2", "EMPLOYER_ADDRESS_1", "EMPLOYER_ADDRESS_2", "EMPLOYER_APPENDIX_B_ATTACHED",
                       "EMPLOYER_CITY", "EMPLOYER_COUNTRY", "EMPLOYER_MSPA_ATTACHED", "EMPLOYER_PHONE",
                       "EMPLOYER_PHONE_EXT", "EMPLOYER_POC_ADDRESS1", "EMPLOYER_POC_ADDRESS2", "EMPLOYER_POC_CITY", "EMPLOYER_POC_COUNTRY",
                       "EMPLOYER_POC_EMAIL", "EMPLOYER_POC_FIRST_NAME", "EMPLOYER_POC_JOB_TITLE", "EMPLOYER_POC_LAST_NAME",
                       "EMPLOYER_POC_MIDDLE_NAME", "EMPLOYER_POC_PHONE", "EMPLOYER_POC_PHONE_EXT", "EMPLOYER_POC_POSTAL_CODE",
                       "EMPLOYER_POC_PROVINCE", "EMPLOYER_POC_STATE", "EMPLOYER_POSTAL_CODE", "EMPLOYER_PROVINCE", "EMPLOYER_STATE",
                       "EMAIL_TO_APPLY", "ATTORNEY_AGENT_ADDRESS_1", "ATTORNEY_AGENT_ADDRESS_2", "ATTORNEY_AGENT_CITY", "ATTORNEY_AGENT_COUNTRY",
                       "ATTORNEY_AGENT_EMAIL", "ATTORNEY_AGENT_EMAIL_ADDRESS", "ATTORNEY_AGENT_FIRST_NAME", "ATTORNEY_AGENT_LAST_NAME",
                       "ATTORNEY_AGENT_MIDDLE_NAME", "ATTORNEY_AGENT_PHONE", "ATTORNEY_AGENT_PHONE_EXT", "ATTORNEY_AGENT_POSTAL_CODE",
                       "ATTORNEY_AGENT_PROVINCE", "ATTORNEY_AGENT_STATE"]

    full_layer = h2a_and_h2b_df.append(additional_housing_df)[columns_to_keep]
    full_layer.to_excel("jobs_for_daniela.xlsx")
    exit()

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
