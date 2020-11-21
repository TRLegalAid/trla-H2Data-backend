import pandas as pd
from helpers import myprint, make_query, get_database_engine
engine = get_database_engine(force_cloud=True)

def fix_previously_fixed():
    previously_fixed_query = """
                            SELECT * FROM (
                        	(SELECT
                            COALESCE("HOUSING_ADDRESS_LOCATION", '') || COALESCE("HOUSING_CITY", '') || COALESCE("HOUSING_STATE", '') || COALESCE("HOUSING_POSTAL_CODE", '') as full_address,
                            id AS low_acc_id
                        	FROM low_accuracies) AS low_acc
                        	INNER JOIN
                        	(SELECT * FROM previously_fixed) AS prev_fixed
                        	ON prev_fixed.initial_address = low_acc.full_address)
                            """

    previously_fixed_df = pd.read_sql(previously_fixed_query, con=engine)
    myprint(f"There are {len(previously_fixed_df)} rows in low_accuracies whose exact housing address column has been fixed before.")

    for fixed in previously_fixed_df.iterrows():
        update_query = f"""
                        UPDATE low_accuracies SET
                        "HOUSING_ADDRESS_LOCATION" = {fixed["HOUSING_ADDRESS_LOCATION"]},
                        "HOUSING_CITY" = {fixed["HOUSING_CITY"]},
                        "HOUSING_POSTAL_CODE" = {fixed["HOUSING_POSTAL_CODE"]},
                        "HOUSING_STATE" = {fixed["HOUSING_STATE"]},
                        "fixed" = {fixed["fixed"]},
                        "housing accuracy" = {fixed["housing accuracy"]},
                        "housing accuracy type" = {fixed["housing accuracy type"]},
                        "housing_fixed_by" = {fixed["housing_fixed_by"]},
                        "housing_lat" = {fixed["housing_lat"]},
                        "housing_long" = {fixed["housing_long"]},
                        "notes" = {fixed["notes"]}
                        WHERE "id" = {fixed["low_acc_id"]}
                        """
        make_query(update_query)

    myprint("Successfully fixed all previously fixed rows in low accuracies.")

if __name__ == "__main__":
   fix_previously_fixed()
