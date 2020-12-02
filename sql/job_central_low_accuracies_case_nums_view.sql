CREATE VIEW job_central_low_accuracies_case_nums AS
	SELECT "CASE_NUMBER" FROM (
		SELECT "CASE_NUMBER" FROM job_central
		UNION
		SELECT "CASE_NUMBER" FROM low_accuracies WHERE "table" != 'dol_h')
		AS case_nums_view
		
