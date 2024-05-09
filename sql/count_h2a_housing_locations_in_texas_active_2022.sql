
/*

Get all unique housing addresses in Texas for jobs active in 2022.

If a case number has null HOUSING_STATE in job_central, it has no additional_housing records
or low_accuracies records (checked on 8/2/23)

H-2A jobs do not last longer than 1 year

All records in low_accuracies are H-2A. There are more than 500 records in low_accuracies with case numbers not present in job_central 


*/

SELECT COUNT(DISTINCT "HOUSING_ADDRESS_LOCATION") 
FROM (
    SELECT jc."HOUSING_ADDRESS_LOCATION"
    FROM job_central AS jc
    WHERE jc."Visa type" = 'H-2A'
    AND jc."HOUSING_STATE" = 'Texas'
    AND (
        jc."EMPLOYMENT_BEGIN_DATE" BETWEEN '2022-01-01' AND '2022-12-31'
        OR jc."EMPLOYMENT_END_DATE" BETWEEN '2022-01-01' AND '2022-12-31'
    )

UNION

SELECT "HOUSING_ADDRESS_LOCATION"
FROM additional_housing 
WHERE "HOUSING_STATE" = 'TEXAS'
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

SELECT "HOUSING_ADDRESS_LOCATION" from low_accuracies
where "HOUSING_STATE" in ('Texas', 'TEXAS')
and (
        "EMPLOYMENT_BEGIN_DATE" BETWEEN '2022-01-01' AND '2022-12-31'
        OR "EMPLOYMENT_END_DATE" BETWEEN '2022-01-01' AND '2022-12-31'
    )

) AS combined_addresses



/* 

How do we handle the records in low_accuracies? 
All records in low_accuracies are H-2A 
There are 500 records in low_accuracies with case numbers that aren't in job central. How is that possible?

We do have the job start and end dates

Both 'Texas' and 'TEXAS' for HOUSING_STATE

There are 235 low_accuracies records with null employment_begin_date, all from DOL
161 + 235 with requested_end_date null 

What's wrong with the below? 235 records don't have begin or end date listed here. WHAT?

49 records in low accuracies with Texas housing location and no employment begin date etc (37 distinct case numbers, 44 distinct addresses)
77 in low accuracies with Texas housing location and valid employment begin date; only 5 are in 2022 ? 

So the question for me really is --- what are these records that have null employment begin date?

*/
