"Script to add local CSV or Excel files to PostgreSQL database."

import pandas as pd
import os
from helpers import get_database_engine, myprint
from dotenv import load_dotenv

load_dotenv()
engine = get_database_engine(force_cloud=True)

# adds file located at path_to_data as table_name. table_name must not exist yet
def add_data_as_table(path_to_data, table_name, excel_or_csv=""):
    if excel_or_csv == "excel":
        data = pd.read_excel(path_to_data)
    elif excel_or_csv == "csv":
        data = pd.read_csv(path_to_data)
    else:
        raise Exception("`excel_or_csv` parameter of add_data_to_table function must be either 'excel' or 'csv'.")

    myprint(f"Adding {len(data)} rows to {table_name}...")
    data.to_sql(table_name, engine, index=False)
    myprint(f"Successfully added the {excel_or_csv} file {path_to_data} as a table named {table_name}.")


if __name__ == '__main__':
    path_to_data = input("Put the file you want to add in the scripts directory. Enter the name of the file (including its extension): ")
    table_name = input(f"Enter the name of the table that will be created from the file {path_to_data}: ")
    excel_or_csv = input(f"Is this an excel or csv file? Enter 'excel' or 'csv' (without quotes): ").lower()
    
    add_data_as_table(path_to_data, table_name, excel_or_csv=excel_or_csv)
