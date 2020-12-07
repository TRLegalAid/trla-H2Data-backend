"""This script is used for implementing the most recent address fixes from the PostgreSQL low_accuracies table."""

import os
import helpers
from helpers import myprint, print_red_and_email, make_query, get_database_engine, handle_null
import pandas as pd
from geocodio import GeocodioClient
from dotenv import load_dotenv
load_dotenv()

geocodio_api_key = os.getenv("GEOCODIO_API_KEY")
engine, client = get_database_engine(force_cloud=True), GeocodioClient(geocodio_api_key)

# fixed is a DataFrame of low_accuracies table rows that have been fixed
def implement_fixes(fixed, fix_worksites=False):

    # worksite_or_housing is a string - either "worksite" or "housing"
    # sets the "{worksite_or_housing}_fixed_by", "fixed" columns to failed, False in the i-th row of df
    def mark_as_failed(i, worksite_or_housing, df):
        df.at[i, f"{worksite_or_housing}_fixed_by"] = "failed"
        df.at[i, "fixed"] = False

    # worksite_or_housing is a string - either "worksite" or "housing"
    # overwrites the geocoding results columns of the i-th row in df based on its worksite_or_housing address columns
    # if the geocoding of the new address columns results in accuracy too low or an accuracy type in helpers.bad_accuracy_types, marks the row as failed
    def fix_by_address(i, row, worksite_or_housing, df):
        if worksite_or_housing == "worksite":
            full_address = helpers.create_address_from(row["WORKSITE_ADDRESS"], row["WORKSITE_CITY"], row["WORKSITE_STATE"], row["WORKSITE_POSTAL_CODE"])
        elif worksite_or_housing == "housing":
            full_address = helpers.create_address_from(row["HOUSING_ADDRESS_LOCATION"], row["HOUSING_CITY"], row["HOUSING_STATE"], row["HOUSING_POSTAL_CODE"])
        else:
            print_red_and_email(f"There was an error fixing the job with case number: {row['CASE_NUMBER']}. worksite_or_housing parameter in fix_by_address must be either `worksite` or `housing`", "Invalid Function Parameter")
            return

        try:
            geocoded = client.geocode(full_address)
            results = geocoded['results'][0]
            df.at[i, f"{worksite_or_housing}_long"] = results['location']['lng']
            df.at[i, f"{worksite_or_housing}_lat"] = results['location']['lat']
            df.at[i, f"{worksite_or_housing} accuracy"] = results['accuracy']
            df.at[i, f"{worksite_or_housing} accuracy type"] = results['accuracy_type']
            if (results['accuracy'] < 0.7) or (results['accuracy_type'] in helpers.bad_accuracy_types):
                print_red_and_email(f"Geocoding the address `{full_address}` (case number {row['CASE_NUMBER']}) resulted in either an accuracy below 0.7 or a bad accuracy type. ", "Fixing Failed")
                mark_as_failed(i, worksite_or_housing, df)
        except Exception as error:
            print_red_and_email(f"Failed to geocode ~{row['CASE_NUMBER']}~ here's the error message:\n{str(error)}", "Geocoding Failure in Implement Fixes")
            mark_as_failed(i, worksite_or_housing, df)

    # worksite_or_housing is a string - either "worksite" or "housing"
    # sets worksite_or_housing accuracy to 1 and worksite_or_housing accuracy type to rooftop in the i-th row of df
    def fix_by_coords(i, worksite_or_housing, df):
        df.at[i, f"{worksite_or_housing} accuracy"] = 1
        df.at[i, f"{worksite_or_housing} accuracy type"] = "rooftop"

    # worksite_or_housing is a string - either "worksite" or "housing"
    # makes sure the worksite_or_housing geocoding results are accurate for the i-th row of df - if they're not, marks that row as failed
    def assert_accuracy(i, row, worksite_or_housing, df):
        table = row["table"]
        if (row["table"] == "dol_h") and (worksite_or_housing == "worksite"):
            return
        elif (row["Visa type"] == "H-2B") and (worksite_or_housing == "housing"):
            return
        else:
            # if checking for housing and h-2a, let it through if all the housing columns are empty
            if worksite_or_housing == "housing" and pd.isna(row["HOUSING_ADDRESS_LOCATION"]) and pd.isna(row["HOUSING_CITY"]) and pd.isna(row["HOUSING_STATE"]) and pd.isna(row["HOUSING_POSTAL_CODE"]):
                print_red_and_email(f"{row['CASE_NUMBER']} is H-2A but all of its housing columns are blank. If its worksite was fixed properly, it will be allowed to pass to job central. This was found while implementing fixes.", "H-2A job Without Housing Data - Implement Fixes")
                return

            if (not row[f"{worksite_or_housing} accuracy type"]) or (row[f"{worksite_or_housing} accuracy"] < 0.7) or (row[f"{worksite_or_housing} accuracy type"] in helpers.bad_accuracy_types):
                print_red_and_email(f"The {worksite_or_housing} data of {row['CASE_NUMBER']} requires fixing, but its {worksite_or_housing}_fixed_by column was not specified to either address, coordinates, inactive, or impossible.", "Address Needs Fixing but Not Fixed")
                mark_as_failed(i, worksite_or_housing, df)

    # worksite_or_housing is a string - either "worksite" or "housing"
    # fixes the i-th row of df based on its worksite_or_housing fixed_by value
    def fix_row(i, row, worksite_or_housing, df):
        method = row[f"{worksite_or_housing}_fixed_by"]
        if method == "address":
            fix_by_address(i, row, worksite_or_housing, df)
        elif method == "coordinates":
            fix_by_coords(i, worksite_or_housing, df)
        elif method == "NA" or pd.isnull(method):
            assert_accuracy(i, row, worksite_or_housing, df)
        elif method == "impossible" or method == "inactive":
            pass
        else:
            error_message = f"Cannot fix job with case number: {row['CASE_NUMBER']}. {worksite_or_housing}_fixed_by column must be either `address`, `coordinates`, `impossible`, `NA`, or null - and it's case sensitive!"
            print_red_and_email(error_message, "Incorrect fixed_by Column Value")
            mark_as_failed(i, worksite_or_housing, df)
            return

    # fixes all rows of df in place - only does it by housing because we're not correcting worksite adddresses as of now
    def fix_rows(df, fix_worksites=False):
        for i, row in df.iterrows():
            fix_row(i, row, "housing", df)
            if fix_worksites:
                fix_row(i, row, "worksite", df)

    # housing_or_central is a string - either "additional_housing" or "job_central"
    # removes columns from the dataframe data that are not in housing_or_central postgres table
    def remove_extra_columns(data, housing_or_central):
        data_columns = data.columns
        housing_or_central_columns = pd.read_sql_query(f'select * from {housing_or_central} limit 1', con=engine).columns
        columns_only_in_data = [column for column in data_columns if column not in housing_or_central_columns]
        return data.drop(columns_only_in_data, axis=1)

    # fixes rows, gets successful fixes, splits into rows for additional_housing and job_central
    fix_rows(fixed)
    success_conditions = (fixed["worksite_fixed_by"] != "failed") & (fixed["housing_fixed_by"] != "failed")
    successes = fixed[success_conditions]
    central = successes[successes["table"] == "central"]
    housing = successes[successes["table"] == "dol_h"]

    # removes columns in central / housing dfs that aren't in theire respective postgres tables
    central = remove_extra_columns(central, "job_central")
    housing = remove_extra_columns(housing, "additional_housing")

    # get failed fixes dataframe
    failure_conditions = (fixed["worksite_fixed_by"] == "failed") | (fixed["housing_fixed_by"] == "failed")
    failures = fixed[failure_conditions]

    return central, housing, failures

# implements fixes from low_accuracies tbable
def send_fixes_to_postgres():
    fixed = pd.read_sql_query('select * from low_accuracies where fixed=true', con=engine)

    if len(fixed) == 0:
        myprint("No jobs have been fixed.")
        return

    myprint(f"{len(fixed)} jobs have been fixed.")
    central, housing, failures = implement_fixes(fixed)
    assert len(central) + len(housing) + len(failures) == len(fixed)
    myprint(f"{len(central)} rows moving from low_accuracies to job_central.")
    myprint(f"{len(housing)} rows moving from low_accuracies to additional housing.")
    myprint(f"{len(failures)} failed fixes.")


    # saving appropriate fixes to previously_fixed table
    prev_fixed_columns = ["HOUSING_ADDRESS_LOCATION", "HOUSING_CITY", "HOUSING_POSTAL_CODE", "HOUSING_STATE", "fixed",
                          "housing accuracy", "housing accuracy type", "housing_fixed_by", "housing_lat",
                          "housing_long", "notes"]
    fixed_by_vals_to_save_fixes_for = ["coordinates", "address", "impossible"]

    central_for_prev_fixed_table = central[central["housing_fixed_by"].isin(fixed_by_vals_to_save_fixes_for)][prev_fixed_columns]
    housing_for_prev_fixed_table = housing[housing["housing_fixed_by"].isin(fixed_by_vals_to_save_fixes_for)][prev_fixed_columns]
    for_prev_fixed_table = central_for_prev_fixed_table.append(housing_for_prev_fixed_table)

    if not for_prev_fixed_table.empty:
        myprint(f"There are {len(for_prev_fixed_table)} rows to add to the previously_fixed table.")
        for_prev_fixed_table["initial_address"] = for_prev_fixed_table.apply(lambda job: handle_null(job["HOUSING_ADDRESS_LOCATION"]) + handle_null(job["HOUSING_CITY"]) + handle_null(job["HOUSING_STATE"]) + handle_null(job["HOUSING_POSTAL_CODE"]), axis=1)
        for_prev_fixed_table = for_prev_fixed_table.drop_duplicates(subset='initial_address', keep="last")
        for_prev_fixed_table.to_sql("previously_fixed", engine, if_exists='append', index=False)
        myprint(f"All rows successfully added to the previously_fixed table.")
    else:
        myprint(f"No rows to add to the previously_fixed table.")

    # adding fixes to appropriate tables and deleting appropriate rows from low_accuracies
    central.to_sql('job_central', engine, if_exists='append', index=False)
    housing.to_sql('additional_housing', engine, if_exists='append', index=False)
    failures.to_sql('low_accuracies', engine, if_exists='append', index=False)
    make_query("delete from low_accuracies where fixed=true")

    myprint(f"Done implementing fixes. There were {len(failures)} failed fixes out of {len(fixed)} attempts.")

if __name__ == "__main__":
   send_fixes_to_postgres()
