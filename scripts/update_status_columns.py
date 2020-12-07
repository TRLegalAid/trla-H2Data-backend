from helpers import make_query

def update_status_columns(table_name):

    make_query(
             f"""UPDATE {table_name}
                    SET "EMPLOYMENT_BEGIN_DATE" = "REQUESTED_BEGIN_DATE"
                    WHERE "EMPLOYMENT_BEGIN_DATE" IS null"""
                )

    make_query(
             f"""UPDATE {table_name}
                    SET "EMPLOYMENT_END_DATE" = "REQUESTED_END_DATE"
                    WHERE "EMPLOYMENT_END_DATE" IS null"""
                )

    make_query(
             f"""UPDATE {table_name}
                    SET status =
                    CASE
         			WHEN ("EMPLOYMENT_BEGIN_DATE" IS null) OR ("EMPLOYMENT_END_DATE" IS null) THEN null
         			WHEN ("EMPLOYMENT_BEGIN_DATE" <= CURRENT_DATE) AND (CURRENT_DATE <= "EMPLOYMENT_END_DATE") THEN 'started'
                    WHEN ("EMPLOYMENT_BEGIN_DATE" > CURRENT_DATE) AND (CURRENT_DATE <= "EMPLOYMENT_END_DATE") THEN 'not yet started'
         			ELSE 'ended'
         			END"""
                )

def update_status_columns_both_tables():
    update_status_columns('job_central')
    update_status_columns('low_accuracies')

if __name__ == "__main__":
    update_status_columns_both_tables()
