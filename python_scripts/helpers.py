from math import isnan

def create_address_from(address, city, state, zip):
    try:
        return address + ", " + city + " " + state + " " + str(zip)
    except:
        return ""

def fix_zip_code(zip_code):
    if isinstance(zip_code, str):
        return ("0" * (5 - len(zip_code))) + zip_code
    elif zip_code == None or isnan(zip_code):
        return None
    else:
        zip_code = str(int(zip_code))
        return ("0" * (5 - len(zip_code))) + zip_code

def fix_zip_code_columns(df, columns):
    for column in columns:
        df[column] = df.apply(lambda job: fix_zip_code(job[column]), axis=1)
