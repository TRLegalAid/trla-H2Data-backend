import pandas as pd
import json
from sqlalchemy import create_engine
import helpers
database_connection_string, geocodio_api_key, _, _, _, _, _, _ = helpers.get_secret_variables()
engine = create_engine(database_connection_string)

# central_cols = pd.read_sql('job_central', con=engine).columns.tolist()
low_acc_cols = pd.read_sql('low_accuracies', con=engine).columns.tolist()
housing_cols = pd.read_sql('additional_housing', con=engine).columns.tolist()
# cols_in_both = [col for col in central_cols if col in low_acc_cols]
cols_in_both = [col for col in housing_cols if col in low_acc_cols]
print(json.dumps(list(cols_in_both)))



# code to fix naics_code columns
# first_8000_h2a = pd.read_excel("DOL_Data/h-2a_q3_first_8000.xlsx", converters={'NAICS_CODE': str})[["NAICS_CODE", "CASE_NUMBER"]]
# last_4000_h2a = pd.read_excel("DOL_Data/h-2a_q3_last_4000.xlsx", converters={'NAICS_CODE': str})[["NAICS_CODE", "CASE_NUMBER"]]
# all_h2a = first_8000_h2a.append(last_4000_h2a).drop_duplicates(subset="CASE_NUMBER")
#
# for i, row in all_h2a.iterrows():
#     case_number = row["CASE_NUMBER"]
#     naics_code = row["NAICS_CODE"]
#
#     with engine.connect() as connection:
#         connection.execute(f"""
#         update job_central set "NAICS_CODE" = '{naics_code}'
#         where "CASE_NUMBER" = '{case_number}' and
#         "table" != 'dol_h'
#         """)
#
#     with engine.connect() as connection:
#         connection.execute(f"""
#         update low_accuracies set "NAICS_CODE" = '{naics_code}'
#         where "CASE_NUMBER" = '{case_number}' and
#         "table" != 'dol_h'
#         """)
#
#     if i % 25 == 0:
#         print(i)
