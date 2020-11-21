import pandas as pd
from helpers import get_database_engine
engine = get_database_engine(force_cloud=True)

job_central_columns = set(pd.read_sql("""SELECT "column_name" FROM information_schema.columns WHERE table_name = 'job_central'""", con=engine)["column_name"].tolist())

# check H-2A columns against existing job central columns
new_h2a_columns = pd.read_excel("dol_data/H-2A_Disclosure_Data_FY2020.xlsx").columns
assert len(set(new_h2a_columns)) == len(new_h2a_columns)
columns_in_new_h2a_but_not_job_central = set(new_h2a_columns) - job_central_columns
print(f"Columns in new H-2A file but not job_central:\n {columns_in_new_h2a_but_not_job_central}\n")

# check H-2B columns against existing job central columns
new_h2b_columns = pd.read_excel("dol_data/H2b_Disclosure_Data_FY2020.xlsx").columns
assert len(set(new_h2b_columns)) == len(new_h2b_columns)
columns_in_new_h2b_but_not_job_central = set(new_h2b_columns) - job_central_columns
print(f"Columns in new H-2B file but not job_central:\n {columns_in_new_h2b_but_not_job_central}\n")

# check H-2A additional housing against additional_housing columns
additional_housing_columns = set(pd.read_sql("""SELECT "column_name" FROM information_schema.columns WHERE table_name = 'additional_housing'""", con=engine)["column_name"].tolist())
new_h2a_housing_columns = pd.read_excel("dol_data/H-2A_AddendumB_Housing_FY2020.xlsx").columns
assert len(set(new_h2a_housing_columns)) == len(new_h2a_housing_columns)
columns_in_new_h2a_housing_but_not_additional_housing_central = set(new_h2a_housing_columns) - additional_housing_columns
print(f"Columns in new additional housing file but not additional_housing:\n {columns_in_new_h2a_housing_but_not_additional_housing_central}\n")


# # check H-2A additional worksites against additional_worksites columns
# additional_worksite_columns = set(pd.read_sql("""SELECT "column_name" FROM information_schema.columns WHERE table_name = 'additional_worksites'""", con=engine)["column_name"].tolist())
# new_h2a_worksite_columns = pd.read_excel("dol_data/H-2A_AddendumB_Employment_FY2020.xlsx").columns
# assert len(set(new_h2a_worksite_columns)) == len(new_h2a_worksite_columns)
# columns_in_new_h2a_worksite_but_not_additional_housing_central = set(new_h2a_worksite_columns) - additional_worksite_columns
# print(f"Columns in new additional worksites file but not additional_housing:\n {columns_in_new_h2a_worksite_but_not_additional_housing_central}\n")
