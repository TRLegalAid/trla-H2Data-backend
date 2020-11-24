import helpers
from helpers import make_query, get_database_engine, myprint
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

engine = get_database_engine(force_cloud=False)

def manage_worksites(worksites, year, quarter):

    worksites = worksites.rename(columns={"PLACE_OF_EMPLOYMENT_ADDRESS1": "WORKSITE_ADDRESS",
                                          "PLACE_OF_EMPLOYMENT_ADDRESS2": "WORKSITE_ADDRESS2",
                                          "PLACE_OF_EMPLOYMENT_CITY": "WORKSITE_CITY",
                                          "PLACE_OF_EMPLOYMENT_STATE": "WORKSITE_STATE",
                                          "PLACE_OF_EMPLOYMENT_POSTAL_CODE": "WORKSITE_POSTAL_CODE"})

    worksites = helpers.fix_zip_code_columns(worksites, ["WORKSITE_POSTAL_CODE"])
    worksites["table"], worksites["Source"], worksites["fy"] = "dol_w", "DOL", f"{year}Q{quarter}"

    return worksites

def add_worksites_to_postgres():
    file_path = "dol_data/" + input("Put the additional worksites file in a folder named `dol_data` in the `scripts` folder. Now enter the name of the file (this is case sensitive).\n").strip()
    year = input("What year is it? (eg: 2020)\n").strip()
    quarter = input("What quarter it is? (enter 1, 2, 3, or 4)\n").strip()
    input(f"Ok, adding worksites from {file_path} for fiscal year {year}Q{quarter}. If this is correct press any key, othewise press control + c to start over.")

    worksites = pd.read_excel(file_path)

    worksites = manage_worksites(worksites, year, quarter)
    myprint(f"Adding {len(worksites)} rows to additional_worksites table.")

    worksites.to_sql("additional_worksites", engine, if_exists='append', index=False)

    if quarter != 1:
        make_query(f"""DELETE FROM additional_worksites WHERE fy = '{year}Q{quarter - 1}'""")

if __name__ == "__main__":
   add_worksites_to_postgres()
