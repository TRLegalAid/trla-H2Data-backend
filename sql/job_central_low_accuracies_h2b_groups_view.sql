CREATE MATERIALIZED VIEW job_central_low_accuracies_h2b_groups AS
    SELECT * FROM job_central_low_accuracies_scraper_fields AS scraper_fields
    LEFT JOIN
    (SELECT "CASE_NUMBER" as groups_case_num, "FY2021Q3_GROUP", "NAICS4", "Title Industry", "Match FY2020"
    FROM h2b_groups) AS groups
    ON scraper_fields."CASE_NUMBER" = groups.groups_case_num;

CREATE UNIQUE INDEX case_num_index ON job_central_low_accuracies_h2b_groups("CASE_NUMBER");

-- to refresh view
REFRESH MATERIALIZED VIEW job_central_low_accuracies_h2b_groups
