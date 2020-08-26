from apscheduler.schedulers.blocking import BlockingScheduler
import pandas as pd
from sqlalchemy import create_engine
import geocodio
import requests
import helpers
import os


def scheduled_job():
    print(helpers.get_secret_variables())


sched = BlockingScheduler()
# change minutes=2 to days=1
sched.add_job(scheduled_job, 'interval', seconds=8, start_date='2020-07-19 22:10:00', timezone='US/Eastern')

sched.start()
