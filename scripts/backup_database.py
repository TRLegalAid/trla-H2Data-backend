"""Script to backup database in the form of local excel files."""

import pandas as pd
from helpers import make_query, get_database_engine
import os
from datetime import datetime
from pytz import timezone
engine = get_database_engine(force_cloud=True)

# outputs all postgres tables as excel files to a folder called database_backups
def backup_database_locally():

    # table_names = pd.read_sql_query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'", con=engine)['table_name'].tolist()
    # table_names = ["additional_housing", "additional_worksites", "h2a_addendum_a", "h2b_appendix_a", "h2b_appendix_c",
    #                "h2b_appendix_d", "job_central", "low_accuracies", "previously_fixed", "pw_disclosure", "pw_disclosure_new", "pw_worksites",
    #                "pw_worksites_new", "raw_scraper_jobs", "recruiters", "spotlight"]
    table_names = ["h2a", "h2b","eta790"]

    if not os.path.exists('../database_backups'):
        os.makedirs('../database_backups')

    # now = datetime.now(tz=timezone('US/Eastern')).strftime("%I.%M%.%S_%p_%B_%d_%Y")
    now = datetime.now(tz=timezone('US/Eastern')).strftime("%B_%d_%Y")

    os.makedirs(f'../database_backups/JSON/{now}')

    for table_name in table_names:
        # table = pd.read_sql(table_name, con=engine).head(30)
        table = pd.read_sql(table_name, con=engine)
        table.to_excel(f"../database_backups/JSON/{now}/{table_name}.xlsx")

if __name__ == "__main__":
   backup_database_locally()
