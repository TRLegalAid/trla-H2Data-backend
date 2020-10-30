import pandas as pd
from helpers import print_red_and_email, column_types, myprint, get_database_engine
from sqlalchemy import create_engine
engine = get_database_engine()

def check_for_duplicates():
    table_names = ["job_central"]
    tables = {}
    for table_name in table_names:
        table = pd.read_sql(table_name, con=engine)
        len_table = len(table)
        without_duplicates = table.drop_duplicates(subset="CASE_NUMBER", keep="last")
        len_without_duplicates = len(without_duplicates)

        if len_table != len_without_duplicates:
            message = f"There are duplciate case numbers in {table_name}. There seem to be {len_table - len_without_duplicates} duplicates ({len_table} rows with duplciates, {len_without_duplicates} without)."
            print_red_and_email(message, "Duplicate case numbers alert")
            # uncomment to remove duplicates from postgres
            # without_duplicates.to_sql(table_name, engine, if_exists='replace', index=False, dtype=column_types)
        else:
            myprint(f"No duplicates in {table_name}.")

if __name__ == "__main__":
    check_for_duplicates()
