from apscheduler.schedulers.blocking import BlockingScheduler
import pandas as pd
from sqlalchemy import create_engine
import geocodio
import requests


def scheduled_job():
    res = requests.get("https://api.heroku.com/apps/for-db/config-vars", headers= {"Accept": "application/vnd.heroku+json; version=3"})
    a_dict = res.json()
    print(a_dict)

sched = BlockingScheduler()
# change minutes=2 to days=1
sched.add_job(scheduled_job, 'interval', seconds=15, start_date='2020-07-19 22:10:00', timezone='US/Eastern')

sched.start()
