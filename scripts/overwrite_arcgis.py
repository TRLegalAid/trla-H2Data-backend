from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection
from colorama import Fore, Style
from helpers import myprint, get_secret_variables
import os
import pandas as pd
from sqlalchemy import create_engine
secrets = get_secret_variables()
DATABASE_URL, ARCGIS_USERNAME, ARCGIS_PASSWORD = secrets[0], secrets[6], secrets[7]
engine = create_engine(DATABASE_URL)

def overwrite_feature(username, password, new_df, old_feature_name):
    gis = GIS(url='https://www.arcgis.com', username=username, password=password)
    # print("Logged in as " + str(gis.properties.user.username))

    csv_file_name = f"{old_feature_name}.csv"
    new_df.to_csv(csv_file_name)

    old_jobs_item = gis.content.search(f"title: {old_feature_name}", 'Feature Layer')[0]
    old_feature_layer = FeatureLayerCollection.fromitem(old_jobs_item)

    myprint(f"Overwriting feature layer.... there will now be {len(new_df)} features.")
    old_feature_layer.manager.overwrite(csv_file_name)
    myprint('Done overwriting feature layer.')

    os.remove(csv_file_name)

job_central_df = pd.read_sql('job_central', con=engine)
def overwrite_our_feature():
    overwrite_feature(ARCGIS_USERNAME, ARCGIS_PASSWORD, job_central_df, 'FW_from_db')

if __name__ == "__main__":
   overwrite_our_feature()
