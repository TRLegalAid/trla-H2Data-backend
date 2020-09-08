import pandas as pd
import os

scraper_jobs = pd.read_excel(os.path.join(os.getcwd(), '..', 'excel_files/all_scraper_data.xlsx'), nrows=3811)
print(scraper_jobs[scraper_jobs["ETA case number"] == "H-300-20029-284509"])

cases = scraper_jobs["ETA case number"].tolist()
set_cases = set(cases)
print(len(cases))
print(len(set_cases))
print(len(cases) - len(set_cases))
