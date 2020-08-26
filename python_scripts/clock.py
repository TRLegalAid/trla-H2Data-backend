from apscheduler.schedulers.blocking import BlockingScheduler
import os
import pandas as pd
from sqlalchemy import create_engine
import geocodio
client = geocodio.GeocodioClient("454565525ee5444fefef2572155e155e5248221")
from populate_database import populate_database

print("made it after the import statemnts of clock.py...")

# def scheduled_job():
#     print("made it into the scheduled job script...")
populate_database()

# sched = BlockingScheduler()
# change minutes=2 to days=1
# sched.add_job(scheduled_job, 'interval', days=1, start_date='2020-08-25 22:10:00', timezone='US/Eastern')
# sched.add_job(scheduled_job, 'interval', days=1)


# sched.start()
