"""Script to add additional H-2A housing DOL data to our database."""

import os
import helpers
from helpers import make_query, get_database_engine
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
engine = get_database_engine(force_cloud=True)

# geocodes, splits by accuracy, renames columns, and adds necessary columns for the DataFrame 'housing'.
# year, quarter should be strings - ex: 2020, 4
def geocode_manage_split_housing(housing, year, quarter):

    housing = housing.rename(columns={"PHYSICAL_LOCATION_ADDRESS_2": "HOUSING_ADDRESS2", "JOB_ORDER_NUMBER": "JO_ORDER_NUMBER",
                                      "PHYSICAL_LOCATION_STATE": "HOUSING_STATE", "PHYSICAL_LOCATION_POSTAL_CODE": "HOUSING_POSTAL_CODE",
                                      "PHYSICAL_LOCATION_CITY": "HOUSING_CITY", "HOUSING_STANDARD_STATE": "HOUSING_STANDARDS_STATE",
                                      "HOUSING_STANDARD_LOCAL": "HOUSING_STANDARDS_LOCAL", "HOUSING_STANDARD_FEDERAL": "HOUSING_STANDARDS_FEDERAL",
                                      "PHYSICAL_LOCATION_COUNTY" : "HOUSING_COUNTY", "PHYSICAL_LOCATION_ADDRESS_1": "HOUSING_ADDRESS_LOCATION"})

    housing = helpers.fix_zip_code_columns(housing, ["HOUSING_POSTAL_CODE"])
    housing["table"], housing["Source"], housing["fixed"], housing["housing_fixed_by"], housing["fy"] = "dol_h", "DOL", None, None, f"{year}Q{quarter}"
    accurate_housing, inaccurate_housing = helpers.geocode_and_split_by_accuracy(housing, table="housing addendum")
    return accurate_housing, inaccurate_housing

# adds housing data from user-inputted excel file
# removes all rows in low_accuracies and additional_housing tables whose table column is dol_h and fy column is the previous quarter,
 # unless the currently inputted quarter is 1
def add_housing_to_postgres():

    file_path = "dol_data/" + input("Check that the additional housing file is in a folder named `dol_data` in the `scripts` folder. (If it isn't, exit this using control + c then re-run this script once you've done it.) Now enter the full name of the file (this is case sensitive and must include the file extension).\n").strip()
    year = input("What year is it? (eg: 2020)\n").strip()
    quarter = input("What quarter it is? (enter 1, 2, 3, or 4)\n").strip()
    input(f"Ok, adding additional housing from {file_path} for fiscal year {year}Q{quarter}. If this is correct press any key, othewise press control + c to start over.")

    housing = pd.read_excel(file_path)
    accurate_housing, inaccurate_housing = geocode_manage_split_housing(housing, year, quarter)

    accurate_housing.to_sql("additional_housing", engine, if_exists='append', index=False)
    inaccurate_housing.to_sql("low_accuracies", engine, if_exists='append', index=False)

    if quarter != 1:
        response = input(f"Enter 'yes' or 'y' if you're ready to run the queries to delete the additional_housing rows from the previous quarter ({year}Q{quarter - 1}). You may want to check that adding the current quarter ({year}Q{quarter}) went well first. If it didn't you can always redo it, but geocoding may cost more money because I won't be able to steal geocoding results from last quarter's additional housing rows.")
        if response.lower() in ["y", "yes"]:
            make_query(f"""DELETE FROM additional_housing WHERE fy = '{year}Q{quarter - 1}'""")
            make_query(f"""DELETE FROM low_accuracies WHERE fy = '{year}Q{quarter - 1}' and "table" = 'dol_h'""")

if __name__ == "__main__":
   add_housing_to_postgres()
