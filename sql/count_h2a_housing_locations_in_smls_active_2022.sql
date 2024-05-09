
/*

Get all unique housing addresses in SMLS states for jobs active in 2022

If a case number has null HOUSING_STATE in job_central, it has no additional_housing records
or low_accuracies records either (checked on 8/2/23)

H-2A jobs do not last longer than 1 year

All records in low_accuracies are H-2A.

*/

SELECT COUNT(DISTINCT "HOUSING_ADDRESS_LOCATION") 
FROM (
    SELECT jc."HOUSING_ADDRESS_LOCATION"
    FROM job_central AS jc
    WHERE jc."Visa type" = 'H-2A'
    AND jc."HOUSING_STATE" IN ('Kentucky', 'Tennessee', 'Alabama', 'Mississippi', 'Louisiana', 'Arkansas')
    AND (
        jc."EMPLOYMENT_BEGIN_DATE" BETWEEN '2022-01-01' AND '2022-12-31'
        OR jc."EMPLOYMENT_END_DATE" BETWEEN '2022-01-01' AND '2022-12-31'
    )

UNION

SELECT "HOUSING_ADDRESS_LOCATION"
FROM additional_housing 
WHERE "HOUSING_STATE" IN ('KENTUCKY', 'TENNESSEE', 'ALABAMA', 'MISSISSIPPI', 'LOUISIANA', 'ARKANSAS')
AND "CASE_NUMBER" IN (
    SELECT "CASE_NUMBER" 
    FROM job_central 
    WHERE "Visa type" = 'H-2A'
    AND (
        "EMPLOYMENT_BEGIN_DATE" BETWEEN '2022-01-01' AND '2022-12-31'
        OR "EMPLOYMENT_END_DATE" BETWEEN '2022-01-01' AND '2022-12-31'
        )
    )

UNION

SELECT "HOUSING_ADDRESS_LOCATION" FROM low_accuracies
WHERE "HOUSING_STATE" IN ('Kentucky', 'KENTUCKY', 'Tennessee', 'TENNESSEE', 'Alabama', 'ALABAMA', 
						 'Mississippi', 'MISSISSIPPI', 'Louisiana', 'LOUISIANA', 'Arkansas', 'ARKANSAS')
AND (
        "EMPLOYMENT_BEGIN_DATE" BETWEEN '2022-01-01' AND '2022-12-31'
        OR "EMPLOYMENT_END_DATE" BETWEEN '2022-01-01' AND '2022-12-31'
    )
) AS combined_addresses