import helpers
from helpers import myprint
import pandas as pd
import numpy as np
import math
from sqlalchemy import create_engine
import psycopg2
from geocodio import GeocodioClient
import numpy as np
database_connection_string, geocodio_api_key, _, _ = helpers.get_secret_variables()
engine, client = create_engine(database_connection_string), GeocodioClient(geocodio_api_key)

def implement_fixes():

    fixed = pd.read_sql_query('select * from "low_accuracies" where fixed=true', con=engine)

    def mark_as_failed(i, worksite_or_housing, df):
        df.at[i, f"{worksite_or_housing}_fixed_by"] = "failed"
        df.at[i, "fixed"] = False

    def fix_by_address(i, row, worksite_or_housing, df):

        if worksite_or_housing == "worksite":
            full_address = helpers.create_address_from(row["WORKSITE_ADDRESS"], row["WORKSITE_CITY"], row["WORKSITE_STATE"], row["WORKSITE_POSTAL_CODE"])
        elif worksite_or_housing == "housing":
            if row["table"] == "central":
                full_address = helpers.create_address_from(row["HOUSING_ADDRESS_LOCATION"], row["HOUSING_CITY"], row["HOUSING_STATE"], row["HOUSING_POSTAL_CODE"])
            elif row["table"] == "dol_h":
                full_address = helpers.create_address_from(row["PHYSICAL_LOCATION_ADDRESS1"], row["PHYSICAL_LOCATION_CITY"], row["PHYSICAL_LOCATION_STATE"], row["PHYSICAL_LOCATION_POSTAL_CODE"])
            else:
                myprint(f"Table column for {job['CASE_NUMBER']} not specified - must be either `dol_h` or `central`", is_red="red")
                mark_as_failed(i, worksite_or_housing, df)
                return
        else:
            myprint(f"There was an error fixing the job with case number: {row['CASE_NUMBER']}. worksite_or_housing must be either `worksite` or `housing`", is_red="red")
            return

        try:
            geocoded = client.geocode(full_address)
            accuracy = geocoded.accuracy
            accuracy_type = geocoded["results"][0]["accuracy_type"]
            df.at[i, f"{worksite_or_housing} coordinates"] = geocoded.coords
            df.at[i, f"{worksite_or_housing} accuracy"] = accuracy
            df.at[i, f"{worksite_or_housing} accuracy type"] = accuracy_type

            if (accuracy < 0.8) or (accuracy_type in helpers.bad_accuracy_types):
                myprint(f"Geocoding {full_address} (case number {row['CASE_NUMBER']}) resulted in either an accuracy below 0.8 or a bad accuracy type. ", is_red="red")
                mark_as_failed(i, worksite_or_housing, df)

        except:
            myprint(f"There was an error geocoding the address: {full_address} (case number{row['CASE_NUMBER']}).", is_red="red")
            mark_as_failed(i, worksite_or_housing, df)

    def fix_by_coords(i, worksite_or_housing, df):
        df.at[i, f"{worksite_or_housing} accuracy"] = 1
        df.at[i, f"{worksite_or_housing} accuracy type"] = "rooftop"

    def assert_accuracy(i, row, worksite_or_housing, df):
        table = row["table"]
        if (row["table"] == "dol_h") and (worksite_or_housing == "worksite"):
            pass
        elif (row["Visa type"] == "H-2B") and (worksite_or_housing == "housing"):
            pass
        else:
            if (not row[f"{worksite_or_housing} accuracy"]) or (not row[f"{worksite_or_housing} accuracy type"]) or (not row[f"{worksite_or_housing} coordinates"]) or (row[f"{worksite_or_housing} accuracy"] < 0.8) or (row[f"{worksite_or_housing} accuracy type"] in helpers.bad_accuracy_types):
                myprint(f"The {worksite_or_housing} data of {row['CASE_NUMBER']} requires fixing, but its {worksite_or_housing}_fixed_by column was not specified to either address, coordinates, or impossible.", is_red="red")
                mark_as_failed(i, worksite_or_housing, df)


    def fix_row(i, row, worksite_or_housing, df):
        method = row[f"{worksite_or_housing}_fixed_by"]
        if method == "address":
            fix_by_address(i, row, worksite_or_housing, df)
        elif method == "coordinates":
            fix_by_coords(i, worksite_or_housing, df)
        elif method == "NA" or method == None:
            assert_accuracy(i, row, worksite_or_housing, df)
        elif method == "impossible":
            pass
        else:
            myprint(f"Cannot fix job with case number: {row['CASE_NUMBER']}. This is because {worksite_or_housing}_fixed_by column must be either `address`, `coordinates`, `impossible`, `NA`, or null - and it's case sensitive!", is_red="red")
            mark_as_failed(i, worksite_or_housing, df)
            return

    def fix_rows(df):
        for i, row in df.iterrows():
            fix_row(i, row, "housing", df)
            fix_row(i, row, "worksite", df)

    fix_rows(fixed)

    # get successfully fixed rows and add them to the big table
    success_conditions = (fixed["worksite_fixed_by"] != "failed") & (fixed["housing_fixed_by"] != "failed")
    successes = fixed[success_conditions]
    central = successes[successes["table"] == "central"]
    housing = successes[successes["table"] == "dol_h"]

    def remove_extra_columns(data, housing_or_central):
        low_accuracies_columns = data.columns
        housing_or_central_columns = pd.read_sql_query(f'select * from {housing_or_central} limit 1', con=engine).columns
        columns_only_in_low_accuracies = [column for column in low_accuracies_columns if column not in housing_or_central_columns]
        return data.drop(columns_only_in_low_accuracies, axis=1)

    central = remove_extra_columns(central, "job_central")
    central = helpers.sort_df_by_date(central)
    central.to_sql('job_central', engine, if_exists='append', index=False, dtype=helpers.column_types)

    housing = remove_extra_columns(housing, "additional_housing")
    housing.to_sql('additional_housing', engine, if_exists='append', index=False, dtype=helpers.column_types)

    # get failed fixes dataframe and not yet fixed dataframe, put them both into low_accuracies table
    failure_conditions = (fixed["worksite_fixed_by"] == "failed") | (fixed["housing_fixed_by"] == "failed")
    failures = fixed[failure_conditions]
    not_fixed = pd.read_sql_query('select * from "low_accuracies" where fixed=false', con=engine)
    failures_and_not_fixed = failures.append(not_fixed, ignore_index=True)
    failures_and_not_fixed = helpers.sort_df_by_date(failures_and_not_fixed)
    failures_and_not_fixed.to_sql('low_accuracies', engine, if_exists='replace', index=False, dtype=helpers.column_types)

implement_fixes()