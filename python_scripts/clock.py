from apscheduler.schedulers.blocking import BlockingScheduler
import os
import pandas as pd
from sqlalchemy import create_engine
import geocodio
client = geocodio.GeocodioClient("454565525ee5444fefef2572155e155e5248221")
from populate_database import populate_database

import logging

print("made it after the import statemnts of clock.py...")

# def scheduled_job():
#     print("made it into the scheduled job script...")

try:
    if x == 4:
        print("hello")
except Exception as e:
    print("There's been a failure, here is the error message:")
    logger.error(e, exc_info=True)


# sched = BlockingScheduler()
# change minutes=2 to days=1
# sched.add_job(scheduled_job, 'interval', days=1, start_date='2020-08-25 22:10:00', timezone='US/Eastern')
# sched.add_job(scheduled_job, 'interval', days=1)


# sched.start()
