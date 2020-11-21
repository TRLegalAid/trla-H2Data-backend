import os
import simplejson as json
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from helpers import get_database_engine, myprint, make_query, convert_date_to_string
from dotenv import load_dotenv
load_dotenv()

engine = get_database_engine(force_cloud=True)
sql_sheet_column_names_map = {"Done?": "fixed", "Company name": "EMPLOYER_NAME", "ETA case number": "CASE_NUMBER",
                              "housing latitude": "housing_lat", "housing longitude": "housing_long",
                              "Housing Address": "HOUSING_ADDRESS_LOCATION", "Housing City": "HOUSING_CITY",
                              "Housing State": "HOUSING_STATE", "Housing Type": "TYPE_OF_HOUSING", "Notes": "notes",
                              "UniqueID": "id", "Date of entry": "Date of run", "Housing Zip Code": "HOUSING_POSTAL_CODE"}

def init_sheets():
    json_creds = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
    creds_dict = json.loads(json_creds)
    creds_dict["private_key"] = creds_dict["private_key"].replace("\\\\n", "\n")
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict)
    client = gspread.authorize(creds)
    # myprint("Google sheet successfully intialized.")
    return client

def open_sheet(client, file_name="", sheet_name=""):
    sheet = client.open(file_name).worksheet(sheet_name)
    myprint(f"Opened worksheet '{sheet_name}' in file '{file_name}'.")
    return sheet

#Writes a pandas dataframe to the current sheet
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
    write_dataframe(sheet, low_accuracies_table)

# sends fixed rows in a sheet to low accuracies table
def send_sheet_fixes_to_postgres(sheet):
    sheet_records = sheet.batch_get(["A:P"])[0]
    column_names = sheet_records.pop(0)
    sheet_df = pd.DataFrame(sheet_records, columns=column_names).rename(columns=sql_sheet_column_names_map)
    sheet_df = sheet_df[sheet_df["fixed"] == "TRUE"]

    if len(sheet_df) == 0:
        myprint("Nobody has fixed any jobs in the Google Sheet.")
        return
    myprint(f"{len(sheet_df)} jobs have been fixed in the Google Sheet. Sending these changes to Postgres now.")

    # sheet_df_only_fixed["housing_lat"] = pd.to_numeric(sheet_df_only_fixed["housing_lat"], errors='coerce')
    sheet_df["housing accuracy"] = pd.to_numeric(sheet_df["housing accuracy"])
    sheet_df["housing_lat"] = pd.to_numeric(sheet_df["housing_lat"])
    sheet_df["housing_long"] = pd.to_numeric(sheet_df["housing_long"])
    sheet_df["id"] = pd.to_numeric(sheet_df["id"])
    sheet_df["Date of run"] = pd.to_datetime(sheet_df["Date of run"])
    sheet_df["fixed"] = sheet_df["fixed"].astype('bool')

    for i, job in sheet_df.iterrows():
        make_query(f"""UPDATE low_accuracies SET
                    (fixed, housing_lat, housing_long, "HOUSING_ADDRESS_LOCATION", "HOUSING_CITY",
                    "HOUSING_STATE", "HOUSING_POSTAL_CODE", notes, housing_fixed_by)
                     =
                     ({job["fixed"]}, {job["housing_lat"]}, {job["housing_long"]}, '{job["HOUSING_ADDRESS_LOCATION"]}',
                     '{job["HOUSING_CITY"]}', '{job["HOUSING_STATE"]}', '{job["HOUSING_POSTAL_CODE"]}', '{job["notes"]}', '{job["housing_fixed_by"]}')
                     WHERE id = {job["id"]}""")


def replace_our_google_sheet_with_low_accuracies_table():
    our_sheet = open_sheet(init_sheets(), file_name="Addresses to Correct", sheet_name="Inbox")
    clear_sheet(our_sheet)
    write_low_accs_to_sheet(our_sheet)

def send_fixes_in_our_google_sheet_to_low_accuracies():
    our_sheet = open_sheet(init_sheets(), file_name="Addresses to Correct", sheet_name="Inbox")
    send_sheet_fixes_to_postgres(our_sheet)

if __name__ == "__main__":
    replace_our_google_sheet_with_low_accuracies_table()
    # send_fixes_in_our_google_sheet_to_low_accuracies()
