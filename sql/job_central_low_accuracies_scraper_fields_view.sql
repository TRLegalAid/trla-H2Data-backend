CREATE VIEW job_central_low_accuracies_scraper_fields AS
	SELECT * FROM (
    		SELECT
            "CASE_NUMBER", "Visa type", "EMPLOYER_NAME", "TRADE_NAME_DBA", "TOTAL_WORKERS_NEEDED",
            "WAGE_OFFER", "Additional Wage Information", "EMPLOYMENT_BEGIN_DATE", "EMPLOYMENT_END_DATE", "EMPLOYER_CITY",
            "EMPLOYER_STATE", "EMPLOYER_POSTAL_CODE", "RECEIVED_DATE", "Experience Required", "Job Order Link", "JOB_ORDER_SUBMIT_DATE",
            "Job Summary Link", "Job duties", "JOB_TITLE", "WORK_EXPERIENCE_MONTHS", "OTHER_WORKSITE_LOCATION", "Number of Workers Requested H-2B",
            "ANTICIPATED_NUMBER_OF_HOURS", "TOTAL_WORKERS_H-2A_REQUESTED", "Job Info/Workers Needed Total", "Place of Employment Info/Address/Location",
            "Place of Employment Info/City", "WORKSITE_COUNTY", "Place of Employment Info/Postal Code", "Place of Employment Info/State",
            "HOUSING_CITY", "HOUSING_COUNTY", "HOUSING_ADDRESS_LOCATION", "TYPE_OF_HOUSING", "HOUSING_POSTAL_CODE", "HOUSING_STATE",
            "TOTAL_OCCUPANCY", "TOTAL_UNITS", "W to H Ratio", "ADDITIONAL_JOB_REQUIREMENTS", "WEBSITE_TO_APPLY", "WORKSITE_ADDRESS",
            "WORKSITE_CITY", "WORKSITE_STATE", "WORKSITE_POSTAL_CODE", "EMAIL_TO_APPLY", "EMPLOYER_PHONE", "PHONE_TO_APPLY", "Number of Pages",
            "Date of run", "Source"
            FROM job_central
    		UNION
    		SELECT
            "CASE_NUMBER", "Visa type", "EMPLOYER_NAME", "TRADE_NAME_DBA", "TOTAL_WORKERS_NEEDED",
            "WAGE_OFFER", "Additional Wage Information", "EMPLOYMENT_BEGIN_DATE", "EMPLOYMENT_END_DATE", "EMPLOYER_CITY",
            "EMPLOYER_STATE", "EMPLOYER_POSTAL_CODE", "RECEIVED_DATE", "Experience Required", "Job Order Link", "JOB_ORDER_SUBMIT_DATE",
            "Job Summary Link", "Job duties", "JOB_TITLE", "WORK_EXPERIENCE_MONTHS", "OTHER_WORKSITE_LOCATION", "Number of Workers Requested H-2B",
            "ANTICIPATED_NUMBER_OF_HOURS", "TOTAL_WORKERS_H-2A_REQUESTED", "Job Info/Workers Needed Total", "Place of Employment Info/Address/Location",
            "Place of Employment Info/City", "WORKSITE_COUNTY", "Place of Employment Info/Postal Code", "Place of Employment Info/State",
            "HOUSING_CITY", "HOUSING_COUNTY", "HOUSING_ADDRESS_LOCATION", "TYPE_OF_HOUSING", "HOUSING_POSTAL_CODE", "HOUSING_STATE",
            "TOTAL_OCCUPANCY", "TOTAL_UNITS", "W to H Ratio", "ADDITIONAL_JOB_REQUIREMENTS", "WEBSITE_TO_APPLY", "WORKSITE_ADDRESS",
            "WORKSITE_CITY", "WORKSITE_STATE", "WORKSITE_POSTAL_CODE", "EMAIL_TO_APPLY", "EMPLOYER_PHONE", "PHONE_TO_APPLY", "Number of Pages",
            "Date of run", "Source"
            FROM low_accuracies WHERE "table" != 'dol_h')
		AS case_nums_view
