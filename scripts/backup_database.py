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
        engine.execute("delete from low_accuracies_backup")
        engine.execute("insert into low_accuracies_backup select * from low_accuracies")
        engine.execute("delete from job_central_backup")
        engine.execute("insert into job_central_backup select * from job_central")



if __name__ == "__main__":
   # backup_database_locally()
   backup_database_on_postgres()
