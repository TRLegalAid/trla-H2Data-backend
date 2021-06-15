"""This script handles the data transfer between our low accuracies google sheet and our low_accuracies PostgreSQL table"""

import os
import simplejson as json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from helpers import get_database_engine, myprint, make_query, convert_date_to_string
from dotenv import load_dotenv
from sqlalchemy.sql import text
load_dotenv()

engine = get_database_engine(force_cloud=True)
# keys are column names in the google sheet, values are column names in the low_accuracies sql table
sql_sheet_column_names_map = {"Done?": "fixed", "Company name": "EMPLOYER_NAME", "ETA case number": "CASE_NUMBER",
                              "housing latitude": "housing_lat", "housing longitude": "housing_long",
                              "Housing Address": "HOUSING_ADDRESS_LOCATION", "Housing City": "HOUSING_CITY",
                              "Housing State": "HOUSING_STATE", "Housing Type": "TYPE_OF_HOUSING", "Notes": "notes",
                              "UniqueID": "id", "Date of entry": "Date of run", "Housing Zip Code": "HOUSING_POSTAL_CODE"}

# returns a client to use for accessing our google sheet
def init_sheets():
    json_creds = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
    creds_dict = json.loads(json_creds)
    creds_dict["private_key"] = creds_dict["private_key"].replace("\\\\n", "\n")
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict)
    client = gspread.authorize(creds)
    return client

# opens the sheet sheet_name from the file file_name using client - returns the opened sheet object
def open_sheet(client, file_name="", sheet_name=""):
    sheet = client.open(file_name).worksheet(sheet_name)
    myprint(f"Opened worksheet '{sheet_name}' in file '{file_name}'.")
    return sheet

# writes a pandas dataframe to sheet
def write_dataframe(sheet, df):
    df.fillna("", inplace=True)
    sheet.update([df.columns.values.tolist()] + df.values.tolist())
    myprint(f"Wrote dataframe to {sheet}")

# clears all values in sheet
def clear_sheet(sheet):
    sheet.clear()

# writes the housing address columns in low_accuracies table to our google sheet
def write_low_accs_to_sheet(sheet):
    low_accuracies_table = pd.read_sql("""SELECT "fixed" as "Done?", "EMPLOYER_NAME" as "Company name", "CASE_NUMBER" as "ETA case number",
                               "housing accuracy", "housing accuracy type", "housing_lat" as "housing latitude",
                               "housing_long" as "housing longitude", "HOUSING_ADDRESS_LOCATION" as "Housing Address",
                               "HOUSING_CITY" as "Housing City", "HOUSING_STATE" as "Housing State", "HOUSING_POSTAL_CODE" as "Housing Zip Code",
                               "TYPE_OF_HOUSING" as "Housing Type", "housing_fixed_by", "notes" as "Notes",
                               "id" as "UniqueID", "Date of run" as "Date of entry"
                               FROM low_accuracies""",
                    con=engine)

    low_accuracies_table["Date of entry"] = low_accuracies_table.apply(lambda row: convert_date_to_string(row["Date of entry"]), axis=1)
    low_accuracies_table["Name"] = ""

    write_dataframe(sheet, low_accuracies_table)

# sends fixed rows in sheet to low accuracies table
def send_sheet_fixes_to_postgres(sheet):
    sheet_records = sheet.batch_get(["A:P"])[0]
    column_names = sheet_records.pop(0)
    sheet_df = pd.DataFrame(sheet_records, columns=column_names).rename(columns=sql_sheet_column_names_map)
    sheet_df = sheet_df[sheet_df["fixed"] == "TRUE"]

    if len(sheet_df) == 0:
        myprint("Nobody has fixed any jobs in the Google Sheet.")
        return
    myprint(f"{len(sheet_df)} jobs have been fixed in the Google Sheet. Sending these changes to Postgres now.")

    sheet_df["housing accuracy"] = pd.to_numeric(sheet_df["housing accuracy"])
    sheet_df["housing_lat"] = pd.to_numeric(sheet_df["housing_lat"])
    sheet_df["housing_long"] = pd.to_numeric(sheet_df["housing_long"])
    sheet_df["id"] = pd.to_numeric(sheet_df["id"])
    sheet_df["Date of run"] = pd.to_datetime(sheet_df["Date of run"])
    sheet_df["fixed"] = sheet_df["fixed"].astype('bool')

    for i, job in sheet_df.iterrows():
        query = text(
                """UPDATE low_accuracies SET
                    (fixed, housing_lat, housing_long, "HOUSING_ADDRESS_LOCATION", "HOUSING_CITY",
                    "HOUSING_STATE", "HOUSING_POSTAL_CODE", notes, housing_fixed_by)
                     =
                     (:fixed, :lat, :long, :address, :city, :state, :zip, :notes, :fixed_by)
                     WHERE id = :id""")

        with engine.connect() as connection:
            connection.execute(query, fixed=job["fixed"], lat=job["housing_lat"], long=job["housing_long"],
                               address=job["HOUSING_ADDRESS_LOCATION"], city=job["HOUSING_CITY"],
                               state=job["HOUSING_STATE"], zip=job["HOUSING_POSTAL_CODE"],
                               notes=job["notes"], fixed_by=job["housing_fixed_by"], id=job["id"])

# clears our google sheet and then writes low_accuracies table to it
def replace_our_google_sheet_with_low_accuracies_table():
    our_sheet = open_sheet(init_sheets(), file_name="Addresses to Correct", sheet_name="Inbox")
    clear_sheet(our_sheet)
    write_low_accs_to_sheet(our_sheet)

# updates low_accuracies table with
def send_fixes_in_our_google_sheet_to_low_accuracies():
    our_sheet = open_sheet(init_sheets(), file_name="Addresses to Correct", sheet_name="Inbox")
    send_sheet_fixes_to_postgres(our_sheet)

if __name__ == "__main__":
    pass
    # replace_our_google_sheet_with_low_accuracies_table()
    send_fixes_in_our_google_sheet_to_low_accuracies()
