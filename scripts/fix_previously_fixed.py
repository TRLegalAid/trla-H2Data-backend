import pandas as pd
from helpers import myprint, make_query, get_database_engine
from sqlalchemy.sql import text
engine = get_database_engine(force_cloud=True)

# Using the previously_fixed table in Postgres, fixes all rows in low_accuracies whose
# addresses have already been fixed
def fix_previously_fixed():
    previously_fixed_query = """
                            SELECT * FROM (
                        	(SELECT
                            COALESCE("HOUSING_ADDRESS_LOCATION", '') || COALESCE("HOUSING_CITY", '') || COALESCE("HOUSING_STATE", '') || COALESCE("HOUSING_POSTAL_CODE", '') as full_address,
                            id AS low_acc_id
                        	FROM low_accuracies) AS low_acc
                        	INNER JOIN
                        	(SELECT DISTINCT ON (initial_address) * FROM previously_fixed) AS prev_fixed
                        	ON prev_fixed.initial_address = low_acc.full_address)
                            """

    previously_fixed_df = pd.read_sql(previously_fixed_query, con=engine)
    myprint(f"There are {len(previously_fixed_df)} rows in low_accuracies whose exact housing address column has been fixed before.")

    for fixed in previously_fixed_df.iterrows():
        update_query = text("""
                        UPDATE low_accuracies SET
                        "HOUSING_ADDRESS_LOCATION" = :address,
                        "HOUSING_CITY" = :city,
                        "HOUSING_POSTAL_CODE" = :zip,
                        "HOUSING_STATE" = :state,
                        "fixed" = :fixed,
                        "housing accuracy" = :accuracy,
                        "housing accuracy type" = :accuracy_type,
                        "housing_fixed_by" = :fixed_by,
                        "housing_lat" = :lat,
                        "housing_long" = :long,
                        "notes" = :notes
                        WHERE "id" = :id
                        """)

        with engine.connect() as connection:
            connection.execute(update_query, address=fixed["HOUSING_ADDRESS_LOCATION"], city=fixed["HOUSING_CITY"],
                              zip=fixed["HOUSING_POSTAL_CODE"], state=fixed["HOUSING_STATE"], fixed=fixed["fixed"],
                              accuracy=fixed["housing accuracy"], accuracy_type=fixed["housing accuracy type"],
                              fixed_by=fixed["housing_fixed_by"], lat=fixed["housing_lat"], long=fixed["housing_long"],
                              notes=fixed["notes"], id=fixed["id"])

    myprint("Successfully fixed all previously fixed rows in low accuracies.")

if __name__ == "__main__":
   fix_previously_fixed()
