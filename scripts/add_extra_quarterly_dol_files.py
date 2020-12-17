import os
import pandas as pd
from helpers import myprint, get_database_engine, make_query
from check_new_dol_columns import check_for_new_columns

engine = get_database_engine(force_cloud=True)
all_table_names = pd.read_sql("""SELECT "tablename" FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'""", engine)["tablename"].tolist()

# appends file_name to table_name, adds fy column, removes rows from postgres where fy = "{year}{quarter - 1}" unless quarter = 1
# year, quarter should be strings - ex: 2020, 4
def append_excel_to_table(file_name, table_name, year, quarter):
    if table_name in all_table_names:
        if check_for_new_columns(file_name, table_name):
            raise Exception(f"Since there are columns in {file_name} that aren't in {table_name}, we can't continue this process or it will fail. Either add the missing columns to PostgreSQL or change the column names in the excel file to match ones that already exist in Postgres.")
    else:
        myprint(f"The table {table_name} doesn't exist yet, so it will be added.")

    df = pd.read_excel(f"dol_data/{file_name}")
    df["fy"] = f"{year}Q{quarter}"

    myprint(f"Adding {len(df)} rows from {file_name} to {table_name}.")

    df.to_sql(table_name, engine, if_exists='append', index=False)
    myprint("Done adding rows.")

    if quarter != 1:
        make_query(f"""DELETE FROM {table_name} WHERE fy = '{year}Q{int(quarter) - 1}'""")


# appends each file in excel_files/dol_table_file_mappings.xlsx to its specified table
def append_excels_to_their_tables():
    year = input("What year is it? (eg: 2020)\n").strip()
    quarter = input("What quarter is it? (enter 1, 2, 3, or 4)\n").strip()
    input(f"Ok, appending excel files for fiscal year {year}Q{quarter}. If this is correct press any key, othewise press control + c to start over.")

    files_to_tables_map = pd.read_excel(os.path.join(os.getcwd(), '..',  "excel_files/dol_table_file_mappings.xlsx"))
    myprint(f"Will be appending {len(files_to_tables_map)} files to their respective tables.")

    for i, pair in files_to_tables_map.iterrows():
        file_name, table_name = pair["file_name"].strip(), pair["table_name"].strip()
        myprint(f"Appending {file_name} to {table_name}.\n")
        try:
            append_excel_to_table(file_name, table_name, year, quarter)
            myprint(f"Success!")
        except Exception as error:
            myprint(f"That didn't work, here's the error message:\n{str(error)}\n")


if __name__ == "__main__":
    append_excels_to_their_tables()
