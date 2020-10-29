from math import isnan
import os
import pandas as pd
from geocodio import GeocodioClient
import requests
import sqlalchemy
from colorama import Fore, Style
from inspect import getframeinfo, stack
import smtplib, ssl
from datetime import datetime
from pytz import timezone
from sqlalchemy import create_engine
from helpers import make_query



def get_secret_variables():
    # LOCAL_DEV is an environment variable that I set to be "true" on my mac and "false" in the heroku config variables
    if os.getenv("LOCAL_DEV") == "true":
        import secret_variables
        return secret_variables.DATABASE_URL, secret_variables.GEOCODIO_API_KEY, secret_variables.MOST_RECENT_RUN_URL, secret_variables.DATE_OF_RUN_URL, secret_variables.ERROR_EMAIL_ADDRESS, secret_variables.ERROR_EMAIL_ADDRESS_PASSWORD, secret_variables.ARCGIS_USERNAME, secret_variables.ARCGIS_PASSWORD
    return os.getenv("DATABASE_URL"), os.getenv("GEOCODIO_API_KEY"), os.getenv("MOST_RECENT_RUN_URL"), os.getenv("DATE_OF_RUN_URL"), os.getenv("ERROR_EMAIL_ADDRESS"), os.getenv("ERROR_EMAIL_ADDRESS_PASSWORD"), os.getenv("ARCGIS_USERNAME"), os.getenv("ARCGIS_PASSWORD")
geocodio_api_key = get_secret_variables()[1]
client = GeocodioClient(geocodio_api_key)




print(pd.isna(None))
print(pd.isna(False))
print(pd.isna(True))
print(pd.isna('ehllo'))





# series = pd.Series([1, 2, 3], index=["a", "b", "c"])
# series["e"] = 6
# print(series)

# df = pd.DataFrame({"A": [1,2], "B": [3, 4]})
# for i, row in df.iterrows():
#     if i == 0:
#         row['alex'] = 6
#         print(row)
#         print(df)
