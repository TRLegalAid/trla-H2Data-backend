import os
import helpers
import pandas as pd
from sqlalchemy import create_engine
from geocodio import GeocodioClient
database_connection_string, geocodio_api_key, _, _, _, _, _, _ = helpers.get_secret_variables()
engine, client = create_engine(database_connection_string), GeocodioClient(geocodio_api_key)

def geocode_manage_split_housing(housing):
    housing = helpers.fix_zip_code_columns(housing, ["PHYSICAL_LOCATION_POSTAL_CODE"])
    housing["table"], housing["Source"], housing["fixed"], housing["housing_fixed_by"] = "dol_h", "DOL", None, None
    accurate_housing, inaccurate_housing = helpers.geocode_and_split_by_accuracy(housing, table="housing addendum")
    return accurate_housing, inaccurate_housing

def add_housing_to_postgres():
    housing = pd.read_excel(os.path.join(os.getcwd(), '..', 'excel_files/h-2a_q3_housing_addendum.xlsx'))
    accurate_housing, inaccurate_housing = geocode_manage_split_housing(housing)
    accurate_housing.to_sql("additional_housing", engine, if_exists='append', index=False, dtype=helpers.column_types)
    with engine.connect() as connection:
        low_accuracies_columns = connection.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'low_accuracies'")
    low_accuracies_columns = [column[0] for column in list(low_accuracies_columns)]
    columns_only_in_addendum = [column for column in housing.columns if column not in low_accuracies_columns]
    query_to_add_columns = ""
    columns_added = 0
    for column in columns_only_in_addendum:
        if columns_added == 0:
            query_to_add_columns += f'ALTER TABLE low_accuracies ADD COLUMN "{column}" text'
        else:
            query_to_add_columns += f', ADD COLUMN "{column}" text'
        columns_added += 1
    # it'll be an empty string if there are no columns in the housing addendum that aren't in postgres yet
    if query_to_add_columns != "":
        with engine.connect() as connection:
            connection.execute(query_to_add_columns)
    inaccurate_housing.to_sql("low_accuracies", engine, if_exists='append', index=False, dtype=helpers.column_types)


if __name__ == "__main__":
   add_housing_to_postgres()
