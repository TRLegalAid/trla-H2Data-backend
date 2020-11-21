import os
import helpers
from helpers import make_query, get_database_engine
import pandas as pd
from sqlalchemy import create_engine
from geocodio import GeocodioClient
from dotenv import load_dotenv
load_dotenv()

geocodio_api_key = os.getenv("GEOCODIO_API_KEY")
engine, client = get_database_engine(), GeocodioClient(geocodio_api_key)

def geocode_manage_split_housing(housing, year, quarter):

    housing = housing.rename(columns={"PHYSICAL_LOCATION_ADDRESS_2": "HOUSING_ADDRESS2", "JOB_ORDER_NUMBER": "JO_ORDER_NUMBER",
                                      "PHYSICAL_LOCATION_STATE": "HOUSING_STATE", "PHYSICAL_LOCATION_POSTAL_CODE": "HOUSING_POSTAL_CODE",
                                      "PHYSICAL_LOCATION_CITY": "HOUSING_CITY", "HOUSING_STANDARD_STATE": "HOUSING_STANDARDS_STATE",
                                      "HOUSING_STANDARD_LOCAL": "HOUSING_STANDARDS_LOCAL", "HOUSING_STANDARD_FEDERAL": "HOUSING_STANDARDS_FEDERAL",
                                      "PHYSICAL_LOCATION_COUNTY" : "HOUSING_COUNTY", "PHYSICAL_LOCATION_ADDRESS_1": "HOUSING_ADDRESS_LOCATION"})

    housing = helpers.fix_zip_code_columns(housing, ["HOUSING_POSTAL_CODE"])
    housing["table"], housing["Source"], housing["fixed"], housing["housing_fixed_by"], housing["fy"] = "dol_h", "DOL", None, None, f"{year}Q{quarter}"
    accurate_housing, inaccurate_housing = helpers.geocode_and_split_by_accuracy(housing, table="housing addendum")
    return accurate_housing, inaccurate_housing

def add_housing_to_postgres():
    file_name, year, quarter = "", 2020, 4
    housing = pd.read_excel(os.path.join(os.getcwd(), '..', file_name))

    accurate_housing, inaccurate_housing = geocode_manage_split_housing(housing, year, quarter)
    accurate_housing.to_sql("additional_housing", engine, if_exists='append', index=False, dtype=helpers.column_types)
    inaccurate_housing.to_sql("low_accuracies", engine, if_exists='append', index=False, dtype=helpers.column_types)

    if quarter != 1:
        make_query(f"""DELETE FROM additional_housing WHERE fy = '{year}Q{quarter - 1}'""")
        make_query(f"""DELETE FROM low_accuracies WHERE fy = '{year}Q{quarter - 1}' and "table" = 'dol_h'""")

if __name__ == "__main__":
   add_housing_to_postgres()
