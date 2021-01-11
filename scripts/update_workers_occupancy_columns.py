"""Script to ensure W to H Ratio and workers_minus_occupancy columns are up to date."""

from helpers import make_query

def update_workers_and_occupancy_columns():

    # merging TOTAL_WORKERS_H-2A_REQUESTED with TOTAL_WORKERS_NEEDED column - this should happen automatically, but just in case something is missed:
    make_query("""
                UPDATE job_central
                    SET "TOTAL_WORKERS_NEEDED" = "TOTAL_WORKERS_H-2A_REQUESTED"
                    WHERE "TOTAL_WORKERS_NEEDED" IS NULL
              """)

    make_query("""
                UPDATE low_accuracies
                    SET "TOTAL_WORKERS_NEEDED" = "TOTAL_WORKERS_H-2A_REQUESTED"
                    WHERE "TOTAL_WORKERS_NEEDED" IS NULL
              """)

    # updating occupancy_minus_workers workers column
    make_query("""
                UPDATE job_central
                    SET occupancy_minus_workers = "TOTAL_OCCUPANCY" - "TOTAL_WORKERS_NEEDED"
                    WHERE "TOTAL_OCCUPANCY" IS NOT NULL AND
                          "TOTAL_WORKERS_NEEDED" IS NOT NULL
              """)

    make_query("""
                UPDATE low_accuracies
                    SET occupancy_minus_workers = "TOTAL_OCCUPANCY" - "TOTAL_WORKERS_NEEDED"
                    WHERE "TOTAL_OCCUPANCY" IS NOT NULL AND
                          "TOTAL_WORKERS_NEEDED" IS NOT NULL
              """)

    # updating W to H Ratio column
    make_query("""
                UPDATE job_central
                    SET "W to H Ratio" =
                    CASE WHEN "TOTAL_WORKERS_NEEDED" > "TOTAL_OCCUPANCY" THEN 'W>H'
                    WHEN "TOTAL_WORKERS_NEEDED" < "TOTAL_OCCUPANCY" THEN 'W<H'
                    ELSE 'W=H'
                    END
                WHERE "TOTAL_OCCUPANCY" IS NOT NULL AND
                      "TOTAL_WORKERS_NEEDED" IS NOT NULL
              """)

    make_query("""
                UPDATE low_accuracies
                    SET "W to H Ratio" =
                    CASE WHEN "TOTAL_WORKERS_NEEDED" > "TOTAL_OCCUPANCY" THEN 'W>H'
                    WHEN "TOTAL_WORKERS_NEEDED" < "TOTAL_OCCUPANCY" THEN 'W<H'
                    ELSE 'W=H'
                    END
                WHERE "TOTAL_OCCUPANCY" IS NOT NULL AND
                      "TOTAL_WORKERS_NEEDED" IS NOT NULL
              """)



if __name__ == "__main__":
    update_workers_and_occupancy_columns()
