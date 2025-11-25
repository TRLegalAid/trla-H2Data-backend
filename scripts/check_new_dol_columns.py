"""Script to check DOL files for columns not present in our database."""

import pandas as pd
from helpers import get_database_engine
from quarterly_column_mappings import RENAME_COLUMNS_DOL_H2A, RENAME_COLUMNS_DOL_H2B, RENAME_COLUMNS_HOUSING, RENAME_COLUMNS_WORKSITES
engine = get_database_engine(force_cloud=True)


def check_for_new_columns(file_name, table_name, rename_mapping):
    postgres_columns = set(pd.read_sql(f"""SELECT "column_name" FROM information_schema.columns WHERE table_name = '{table_name}'""", con=engine)["column_name"].tolist())

    file_columns = pd.read_excel(f"dol_data/{file_name}").columns
    assert len(set(file_columns)) == len(file_columns), "There are duplicate column names in the excel file, which is bad."
    
    previously_renamed_columns = set(rename_mapping.keys())

    new_columns = postgres_columns - (postgres_columns | previously_renamed_columns)

    print(f"Columns in new {file_name} file but not {table_name} or renamed in quarterly_column_mappings.py:\n {new_columns}\n")
    return new_columns



# reports any columns in the H2A additional_worksites, H2B additional_housings, H2A disclosure, and
# H2B disclosure files that are not in these files' corresponding tables in Postgres. Edit `files_tables_map`
# accordingly based on file names. Assumes all files are located in a folder named dol_data which is in this directory.
if __name__ == "__main__":

    # files_tables_map = {
    #     "H-2A_Disclosure_Data_FY2025_Q1.xlsx": "job_central",
    #     "H-2B_Disclosure_Data_FY2025_Q1.xlsx": "job_central",
    #     "H-2A_Addendum_B_Housing_FY2025_Q1.xlsx": "additional_housing",
    #     "H-2A_Addendum_B_Employ_FY2025_Q1.xlsx": "additional_worksites"
    #     }
    
    files_tables_map = {
        "H-2A_Disclosure_Data_FY2025_Q3.xlsx": {
            "table": "job_central", 
            "rename_mapping": RENAME_COLUMNS_DOL_H2A
        },
        "H-2B_Disclosure_Data_FY2025_Q3.xlsx": {
            "table": "job_central",
            "rename_mapping": RENAME_COLUMNS_DOL_H2B
        },
        "H-2A_Addendum_B_Housing_FY2025_Q3.xlsx": {
            "table": "additional_housing", 
            "rename_mapping": RENAME_COLUMNS_HOUSING
        },
        "H-2A_Addendum_B_Employ_FY2025_Q3.xlsx": {
            "table": "additional_worksites", 
            "rename_mapping": RENAME_COLUMNS_WORKSITES
        }

    }


    for file, file_metadata in files_tables_map.items():
        check_for_new_columns(file, file_metadata["table"], file_metadata["rename_mapping"])
