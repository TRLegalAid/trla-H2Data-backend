import os
import helpers
from helpers import myprint, print_red_and_email, make_query, get_database_engine
import pandas as pd
from sqlalchemy import create_engine
from geocodio import GeocodioClient
if os.getenv("LOCAL_DEV") == "true":
    from dotenv import load_dotenv
    load_dotenv()
geocodio_api_key = os.getenv("GEOCODIO_API_KEY")
engine, client = get_database_engine(force_cloud=True), GeocodioClient(geocodio_api_key)

def implement_fixes(fixed):

    def mark_as_failed(i, worksite_or_housing, df):
        df.at[i, f"{worksite_or_housing}_fixed_by"] = "failed"
        df.at[i, "fixed"] = False

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
            if (results['accuracy'] < 0.8) or (results['accuracy_type'] in helpers.bad_accuracy_types):
                print_red_and_email(f"Geocoding the address `{full_address}` (case number {row['CASE_NUMBER']}) resulted in either an accuracy below 0.8 or a bad accuracy type. ", "Fixing Failed")
                mark_as_failed(i, worksite_or_housing, df)
        except Exception as error:
            print_red_and_email(f"Failed to geocode ~{row['CASE_NUMBER']}~ here's the error message:\n{str(error)}", "Geocoding Failure in Implement Fixes")
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
            # if checking for housing and h-2a, let it through if all the housing columns are empty
            if worksite_or_housing == "housing" and pd.isna(job["HOUSING_ADDRESS_LOCATION"]) and pd.isna(job["HOUSING_CITY"]) and pd.isna(job["HOUSING_STATE"]) and pd.isna(job["HOUSING_POSTAL_CODE"]):
                print_red_and_email(f"{job['CASE_NUMBER']} is H-2A but all of its housing columns are blank. If its worksite was fixed properly, it will be allowed to pass to job central. This was found while implementing fixes.", "H-2A job Without Housing Data - Implement Fixes")
                pass

            if (not row[f"{worksite_or_housing} accuracy type"]) or (row[f"{worksite_or_housing} accuracy"] < 0.8) or (row[f"{worksite_or_housing} accuracy type"] in helpers.bad_accuracy_types):
                print_red_and_email(f"The {worksite_or_housing} data of {row['CASE_NUMBER']} requires fixing, but its {worksite_or_housing}_fixed_by column was not specified to either address, coordinates, inactive, or impossible.", "Address Needs Fixing but Not Fixed")
                mark_as_failed(i, worksite_or_housing, df)

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
            error_message = f"Cannot fix job with case number: {row['CASE_NUMBER']}. This is because {worksite_or_housing}_fixed_by column must be either `address`, `coordinates`, `impossible`, `NA`, or null - and it's case sensitive!"
            print_red_and_email(error_message, "Incorrect fixed_by Column Value")
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
    housing = remove_extra_columns(housing, "additional_housing")
    # get failed fixes dataframe and not yet fixed dataframe, put them both into low_accuracies table
    failure_conditions = (fixed["worksite_fixed_by"] == "failed") | (fixed["housing_fixed_by"] == "failed")
    failures = fixed[failure_conditions]
    return central, housing, failures

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

    central.to_sql('job_central', engine, if_exists='append', index=False, dtype=helpers.column_types)
    housing.to_sql('additional_housing', engine, if_exists='append', index=False, dtype=helpers.column_types)
    failures.to_sql('low_accuracies', engine, if_exists='append', index=False, dtype=helpers.column_types)

    make_query("delete from low_accuracies where fixed=true")
    myprint(f"Done implementing fixes. There were {len(failures)} failed fixes out of {len(fixed)} attempts.")

if __name__ == "__main__":
   send_fixes_to_postgres()
