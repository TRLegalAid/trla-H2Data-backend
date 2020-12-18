"""Mappings of Apify column names to PostgreSQL column names."""

# keys are apify field names, values are our PostgreSQL field names
# used in update_database.py
column_name_mappings = {'ETA case number': 'CASE_NUMBER', 'Company name': 'EMPLOYER_NAME',
                        'Trade Name/Doing Business As': 'TRADE_NAME_DBA', 'Base salary': 'WAGE_OFFER',
                        'Begin date': 'EMPLOYMENT_BEGIN_DATE', 'End date': 'EMPLOYMENT_END_DATE',
                        'Company city': 'EMPLOYER_CITY', 'Company state': 'EMPLOYER_STATE',
                        'Company zip code': 'EMPLOYER_POSTAL_CODE', 'Date posted': 'RECEIVED_DATE',
                        'Job title': 'JOB_TITLE', 'Months of Experience Required': 'WORK_EXPERIENCE_MONTHS',
                        'Multiple Worksites': 'OTHER_WORKSITE_LOCATION', 'Number of Workers Requested': 'TOTAL_WORKERS_NEEDED',
                        'Number of hours per week': 'ANTICIPATED_NUMBER_OF_HOURS',
                        'Job Info/Workers Needed H-2A': 'TOTAL_WORKERS_H-2A_REQUESTED',
                        'Place of Employment Info/County': 'WORKSITE_COUNTY',
                        'Housing Info/Housing Address': 'HOUSING_ADDRESS_LOCATION', 'Housing Info/City': 'HOUSING_CITY',
                        'Housing Info/State': 'HOUSING_STATE', 'Housing Info/Postal Code': 'HOUSING_POSTAL_CODE',
                        'Housing Info/County': 'HOUSING_COUNTY', 'Housing Info/Housing Type': 'TYPE_OF_HOUSING',
                        'Housing Info/Total Occupancy': 'TOTAL_OCCUPANCY', 'Housing Info/Total Units': 'TOTAL_UNITS',
                        'Special requirements': 'ADDITIONAL_JOB_REQUIREMENTS', 'Web address to apply': 'WEBSITE_TO_APPLY',
                         'Worksite address': 'WORKSITE_ADDRESS', 'Worksite address city': 'WORKSITE_CITY',
                         'Worksite address state': 'WORKSITE_STATE', 'Worksite address zip code': 'WORKSITE_POSTAL_CODE',
                         'e-mail to apply': 'EMAIL_TO_APPLY', 'Employer Telephone number': 'EMPLOYER_PHONE',
                         'Telephone number to apply': 'PHONE_TO_APPLY', 'Job Posted At': 'JOB_ORDER_SUBMIT_DATE'}
