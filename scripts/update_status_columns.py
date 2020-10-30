from helpers import make_query

def update_status_columns():
    make_query(
             """UPDATE job_central
                    SET status =
                    CASE
         			WHEN ("EMPLOYMENT_BEGIN_DATE" IS null) OR ("EMPLOYMENT_END_DATE" IS null) THEN null
         			WHEN ("EMPLOYMENT_BEGIN_DATE" <= CURRENT_DATE) AND (CURRENT_DATE <= "EMPLOYMENT_END_DATE") THEN 'active'
         			ELSE 'inactive'
         			END"""
                )

    make_query(
             """UPDATE low_accuracies
                    SET status =
                    CASE
         			WHEN ("EMPLOYMENT_BEGIN_DATE" IS null) OR ("EMPLOYMENT_END_DATE" IS null) THEN null
         			WHEN ("EMPLOYMENT_BEGIN_DATE" <= CURRENT_DATE) AND (CURRENT_DATE <= "EMPLOYMENT_END_DATE") THEN 'active'
         			ELSE 'inactive'
         			END"""
                )


if __name__ == "__main__":
    update_status_columns()
