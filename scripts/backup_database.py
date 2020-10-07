import pandas as pd
import helpers
import os
from sqlalchemy import create_engine
from datetime import datetime
from pytz import timezone
database_connection_string, _, _, _, _, _, _, _ = helpers.get_secret_variables()
engine = create_engine(database_connection_string)

def backup_database_locally():

    table_names = pd.read_sql_query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'", con=engine)['table_name'].tolist()

    if not os.path.exists('../database_backups'):
        os.makedirs('../database_backups')

    now = datetime.now(tz=timezone('US/Eastern')).strftime("%I.%M%.%S_%p_%B_%d_%Y")
    os.makedirs(f'../database_backups/{now}')

    for table_name in table_names:
        table = pd.read_sql(table_name, con=engine)
        table.to_excel(f"../database_backups/{now}/{table_name}.xlsx")


def backup_database_on_postgres():
    with engine.connect() as connection:
        # engine.execute("delete from low_accuracies_backup")
        # engine.execute("insert into low_accuracies_backup select * from low_accuracies")
        #
        # engine.execute("delete from job_central_backup")
        # engine.execute("insert into job_central_backup select * from job_central")
        #
        # engine.execute("delete from additional_housing_backup")
        # engine.execute("insert into additional_housing_backup select * from additional_housing")

        engine.execute("drop table low_accuracies_backup")
        engine.execute("CREATE TABLE low_accuracies_backup AS TABLE low_accuracies")

        engine.execute("drop table job_central_backup")
        engine.execute("CREATE TABLE job_central_backup AS TABLE job_central")

        engine.execute("drop table additional_housing_backup")
        engine.execute("CREATE TABLE additional_housing_backup AS TABLE additional_housing")

if __name__ == "__main__":
   # backup_database_locally()
   backup_database_on_postgres()
