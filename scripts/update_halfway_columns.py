from helpers import make_query

# updates days_until_halfway and is_halfway columns in job_central and low_accuracies PostgreSQL tables
def update_halfway_columns():

    # update days_until_halfway column for job_central and low_accuracies
    make_query("""
                UPDATE job_central
                	SET days_until_halfway =
                	CASE WHEN status != 'ended' THEN
                		EXTRACT(DAY FROM ("EMPLOYMENT_BEGIN_DATE" + (("EMPLOYMENT_END_DATE" - "EMPLOYMENT_BEGIN_DATE") / 2)) - CURRENT_DATE)
                	ELSE NULL
                	END
              """)

    make_query("""
                UPDATE low_accuracies
                	SET days_until_halfway =
                	CASE WHEN status != 'ended' THEN
                		EXTRACT(DAY FROM ("EMPLOYMENT_BEGIN_DATE" + (("EMPLOYMENT_END_DATE" - "EMPLOYMENT_BEGIN_DATE") / 2)) - CURRENT_DATE)
                	ELSE NULL
                	END
                WHERE "table" = 'central'
               """)

    # update is_halfway column for job_central and low_accuracies
    make_query("""
                UPDATE job_central
                	SET is_halfway =
                	CASE WHEN days_until_halfway > 0 THEN FALSE
                	WHEN days_until_halfway IS NULL THEN NULL
                	ELSE TRUE
                	END
               """)

    make_query("""
                UPDATE low_accuracies
                	SET is_halfway =
                	CASE WHEN days_until_halfway > 0 THEN FALSE
                	WHEN days_until_halfway IS NULL THEN NULL
                	ELSE TRUE
                	END
                WHERE "table" = 'central'
               """)


if __name__ == "__main__":
    update_halfway_columns()
