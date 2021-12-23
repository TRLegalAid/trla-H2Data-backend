from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
load_dotenv()

def get_database_engine(force_cloud=False):
    if force_cloud or (not (os.getenv("LOCAL_DEV") == "true")):
        return create_engine(os.getenv("DATABASE_URL_NEW"))
    else:
        return create_engine(os.getenv("LOCAL_DATABASE_URL"))


engine = get_database_engine(force_cloud=True)
print("success")
exit()
