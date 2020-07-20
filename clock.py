from apscheduler.schedulers.blocking import BlockingScheduler
import pandas as pd
from sqlalchemy import create_engine
import geocodio
client = geocodio.GeocodioClient("454565525ee5444fefef2572155e155e5248221")
import requests

engine = create_engine('postgres://txmzafvlwebrcr:df20d17265cf81634b9f689187248524a6fd0d56222985e2f422c71887ec6ec0@ec2-34-224-229-81.compute-1.amazonaws.com:5432/dbs39jork6o07d')

def scheduled_job():
    print("hello")

sched = BlockingScheduler()
# change minutes=2 to days=1
sched.add_job(scheduled_job, 'interval', seconds=30, start_date='2020-07-19 22:10:00', timezone='US/Eastern')

sched.start()
