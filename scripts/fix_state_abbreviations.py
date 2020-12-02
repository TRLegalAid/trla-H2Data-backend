import os
import pandas as pd
from helpers import make_query


states_abbreviations = {'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California',
                        'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware', 'DC': 'District of Columbia',
                        'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana',
                        'IA': 'Iowa', 'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
                        'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi', 'MO': 'Missouri',
                        'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire', 'NJ': 'New Jersey',
                        'NM': 'New Mexico', 'NY': 'New York', 'NC': 'North Carolina', 'ND': 'North Dakota',
                        'OH': 'Ohio', 'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island',
                        'SC': 'South Carolina', 'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
                        'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia', 'WI': 'Wisconsin',
                        'WY': 'Wyoming', 'PR': 'Puerto Rico', 'MP': 'Northern Mariana Islands', 'VI': 'U.S. Virgin Islands'}


# converts all abbreviations in the WORKSITE_STATE column of job_central to full state names
def expand_abbreviations():

    for abbreviation in states_abbreviations:
        state = states_abbreviations[abbreviation]
        make_query(f"""UPDATE job_central
                       SET "WORKSITE_STATE" = '{state}'
                       WHERE "WORKSITE_STATE" IN ('{abbreviation}', '{abbreviation.lower()}')""")

        make_query(f"""UPDATE low_accuracies
                       SET "WORKSITE_STATE" = '{state}'
                       WHERE "WORKSITE_STATE" IN ('{abbreviation}', '{abbreviation.lower()}')""")

        make_query(f"""UPDATE job_central
                       SET "HOUSING_STATE" = '{state}'
                       WHERE "HOUSING_STATE" IN ('{abbreviation}', '{abbreviation.lower()}')""")

        make_query(f"""UPDATE additional_housing
                       SET "HOUSING_STATE" = '{state}'
                       WHERE "HOUSING_STATE" IN ('{abbreviation}', '{abbreviation.lower()}')""")

        make_query(f"""UPDATE low_accuracies
                       SET "HOUSING_STATE" = '{state}'
                       WHERE "HOUSING_STATE" IN ('{abbreviation}', '{abbreviation.lower()}')""")

if __name__ == "__main__":
   expand_abbreviations()
