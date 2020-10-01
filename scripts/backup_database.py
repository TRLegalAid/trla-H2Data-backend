import pandas as pd
import helpers
import os
from sqlalchemy import create_engine
database_connection_string, _, _, _, _, _, _, _ = helpers.get_secret_variables()
engine = create_engine(database_connection_string)

table_names = pd.read_sql_query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'", con=engine)['table_name'].tolist()

if not os.path.exists('database_backups'):
    os.makedirs('database_backups')

for table_name in table_names:
    table = pd.read_sql(table_name, con=engine)
    table.to_excel(f"database_backups/{table_name}.xlsx")
