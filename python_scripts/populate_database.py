import os
import pandas as pd
import sqlalchemy
import helpers
from sqlalchemy import create_engine
database_connection_string = helpers.get_secret_variables()[0]
engine = create_engine(database_connection_string)

def populate_database():
    print("made it into the populate database script...")
    renaming_info_dict = {"Section A": "Job Info", "Section C": "Place of Employment Info", "Section D":"Housing Info"}
    column_names_dict = {}
    df = pd.read_excel(os.path.join(os.getcwd(), '..', 'excel_files/scraper_data.xlsx'))
    df = pd.read_excel(os.path.join(os.getcwd(), '..', 'excel_files/scraper_data_big.xlsx'))
    for column in df.columns:
        for key in renaming_info_dict:
            if key in column:
                if column == "Section C/Address/Location":
                    column_names_dict[column] = "Place of Employment Info/Address/Location"
                else:
                    column_names_dict[column] = renaming_info_dict[key] + "/" + column.split("/")[1]

    df = df.rename(columns=column_names_dict)
    df = df.drop(columns=["Telephone number"])
    df = helpers.rename_columns(df)
    df = df.drop_duplicates(subset='CASE_NUMBER', keep="last")

    df["fixed"], df["worksite_fixed_by"], df["housing_fixed_by"], df["notes"], df["table"] = None, None, None, None, "central"
    helpers.fix_zip_code_columns(df, ["EMPLOYER_POSTAL_CODE", "WORKSITE_POSTAL_CODE",  "Place of Employment Info/Postal Code", "HOUSING_POSTAL_CODE"])
    accurate_jobs, inaccurate_jobs = helpers.geocode_and_split_by_accuracy(df)

    accurate_jobs.to_sql("job_central", engine, if_exists='replace', index=False, dtype={"Experience Required": sqlalchemy.types.Boolean, "Multiple Worksites": sqlalchemy.types.Boolean})
    inaccurate_jobs.to_sql("low_accuracies", engine, if_exists='replace', index=False, dtype={"Experience Required": sqlalchemy.types.Boolean, "Multiple Worksites": sqlalchemy.types.Boolean})
