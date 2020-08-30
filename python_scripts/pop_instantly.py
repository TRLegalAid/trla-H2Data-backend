import os
import pandas as pd
import sqlalchemy
import helpers
from sqlalchemy import create_engine
database_connection_string = helpers.get_secret_variables()[0]
engine = create_engine(database_connection_string)



accurate_jobs = pd.read_excel(os.path.join(os.getcwd(), '..', 'excel_files/accurates_geocoded.xlsx'))
inaccurate_jobs = pd.read_excel(os.path.join(os.getcwd(), '..', 'excel_files/inaccurates_geocoded.xlsx'))


accurate_jobs.to_sql("job_central", engine, if_exists='replace', index=False, dtype={"Experience Required": sqlalchemy.types.Boolean, "Multiple Worksites": sqlalchemy.types.Boolean, "fixed": sqlalchemy.types.Boolean})
inaccurate_jobs.to_sql("low_accuracies", engine, if_exists='replace', index=False, dtype={"Experience Required": sqlalchemy.types.Boolean, "Multiple Worksites": sqlalchemy.types.Boolean, "fixed": sqlalchemy.types.Boolean})
