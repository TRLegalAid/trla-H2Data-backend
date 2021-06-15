CREATE MATERIALIZED VIEW latest_cases AS
    SELECT * FROM job_central_low_accuracies_h2b_groups
    WHERE "Date of run" >= CURRENT_DATE;

CREATE UNIQUE INDEX case_num_index_latest_cases ON latest_cases("CASE_NUMBER");

-- to refresh view
REFRESH MATERIALIZED VIEW latest_cases
