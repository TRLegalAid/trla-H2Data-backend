import pandas as pd
import json
from sqlalchemy import create_engine
database_connection_string, geocodio_api_key, _, _, _, _, _, _ = helpers.get_secret_variables()
engine = create_engine(database_connection_string)

# central_cols = pd.read_sql('job_central', con=engine).columns.tolist()
low_acc_cols = pd.read_sql('low_accuracies', con=engine).columns.tolist()
housing_cols = pd.read_sql('additional_housing', con=engine).columns.tolist()
# cols_in_both = [col for col in central_cols if col in low_acc_cols]
cols_in_both = [col for col in housing_cols if col in low_acc_cols]
print(json.dumps(list(cols_in_both)))
