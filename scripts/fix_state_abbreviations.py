import os
import pandas as pd
from helpers import make_query

# converts all abbreviations in the WORKSITE_STATE column of job_central to full state names
def expand_abbreviations():
    states_with_abbreviations = pd.read_excel("../excel_files/state_abbreviations.xlsx")

    for i, row in states_with_abbreviations.iterrows():
        state, abbreviation = row["State"], row["Abbreviation"]
        make_query(f"""UPDATE job_central
                       SET "WORKSITE_STATE" = '{state}'
                       WHERE "WORKSITE_STATE" IN ('{abbreviation}', '{abbreviation.lower()}')""")

if __name__ == "__main__":
   expand_abbreviations()
