import pandas as pd
from helpers import make_query, get_database_engine
import os
from sqlalchemy import create_engine
from datetime import datetime
from pytz import timezone
engine = get_database_engine(force_cloud=True)

def backup_database_locally():

    table_names = pd.read_sql_query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'", con=engine)['table_name'].tolist()

    if not os.path.exists('../database_backups'):
        os.makedirs('../database_backups')

    now = datetime.now(tz=timezone('US/Eastern')).strftime("%I.%M%.%S_%p_%B_%d_%Y")
    os.makedirs(f'../database_backups/{now}')

    for table_name in table_names:
        table = pd.read_sql(table_name, con=engine).head(30)
        table.to_excel(f"../database_backups/{now}/{table_name}.xlsx")


def backup_database_on_postgres():
    make_query("drop table low_accuracies_backup")
    make_query("CREATE TABLE low_accuracies_backup AS TABLE low_accuracies")

    make_query("drop table job_central_backup")
    make_query("CREATE TABLE job_central_backup AS TABLE job_central")

    make_query("drop table additional_housing_backup")
    make_query("CREATE TABLE additional_housing_backup AS TABLE additional_housing")

if __name__ == "__main__":
   # backup_database_on_postgres()
   backup_database_locally()
