# spreadsheet_id = 1qNK57DTebJstUwMyZBH3cc_5mMrvTJm1ZgiKGn2F2kg
# sheet_id = 0

import os
import simplejson as json
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
import pandas as pd
from helpers import get_database_engine, myprint
from datetime import datetime
engine = get_database_engine(force_cloud=True)

load_dotenv()


def init_sheets():
    json_creds = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
    creds_dict = json.loads(json_creds)
    creds_dict["private_key"] = creds_dict["private_key"].replace("\\\\n", "\n")
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict)
    client = gspread.authorize(creds)
    myprint("Google sheet successfully intialized.")
    return client

def open_sheet(client, file_name="", sheet_name=""):
    sheet = client.open(file_name).worksheet(sheet_name)
    myprint(f"Opened worksheet '{sheet_name}' in file '{file_name}'.")
    return sheet

#Reads data into a pandas dataframe from the current sheet
def read_data(sheet):
    df = pd.DataFrame(sheet.get_all_records())
    myprint(f"Data read from {sheet}")
    return df

#Writes a pandas dataframe to the current sheet
def write_dataframe(sheet, df):
    df.fillna("", inplace=True)
    sheet.update([df.columns.values.tolist()] + df.values.tolist())
    myprint(f"Wrote data to {sheet}")






client = init_sheets()
# sheet = open_sheet(client, file_name="testing", sheet_name="test")
sheet = open_sheet(client, file_name="Addresses to Correct", sheet_name="Inbox")
# instead of 10000 try to get the number of records in the sheet
values = [[" "] * 17] * 10000
sheet.update(values)

df = pd.read_sql("""SELECT "fixed" as "Done?", "EMPLOYER_NAME" as "Company name", "CASE_NUMBER" as "ETA case number",
                           "housing accuracy", "housing accuracy type", "housing_lat" as "housing latitude",
                           "housing_long" as "housing longitude", "HOUSING_ADDRESS_LOCATION" as "Housing Address",
                           "HOUSING_CITY" as "Housing City", "HOUSING_STATE" as "Housing State",
                           "TYPE_OF_HOUSING" as "Housing Type", "housing_fixed_by", "notes" as "Notes",
                           "id" as "UniqueID", "Date of run" as "Date of entry"
                           from low_accuracies""",
                con=engine)


def convert_date_to_string(date):
    if pd.isna(date):
        return ""
    return date.strftime("%m/%d/%Y, %H:%M:%S")
df["Date of entry"] = df.apply(lambda row: convert_date_to_string(row["Date of entry"]), axis=1)
write_dataframe(sheet, df)
