from apscheduler.schedulers.blocking import BlockingScheduler
import pandas as pd
from sqlalchemy import create_engine
from geocodio import GeocodioClient
client = GeocodioClient("454565525ee5444fefef2572155e155e5248221")
import requests

engine = create_engine('postgres://txmzafvlwebrcr:df20d17265cf81634b9f689187248524a6fd0d56222985e2f422c71887ec6ec0@ec2-34-224-229-81.compute-1.amazonaws.com:5432/dbs39jork6o07d')

def scheduled_job():
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    df.to_sql("new3", engine, if_exists='append', index=False)
    print("maybe this succeeded")
    geocoded = client.geocode("11 Cortlandt Manor Rd, Katonah NY 10536")
    print(geocoded.coords)
    jobs = requests.get("https://api.apify.com/v2/acts/eytaog~apify-dol-actor-latest/runs/last/dataset/items?token=ftLRsXTA25gFTaCvcpnebavKw").json()
    print(len(jobs))


sched = BlockingScheduler()
# change minutes=2 to days=1
sched.add_job(scheduled_job, 'interval', minutes=2, start_date='2020-07-19 16:30:00', timezone='US/Eastern')

sched.start()
