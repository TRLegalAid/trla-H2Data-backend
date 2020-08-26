from apscheduler.schedulers.blocking import BlockingScheduler
import os
from helpers import get_secret_variables
print(get_secret_variables()[1])
from geocodio import GeocodioClient
client = GeocodioClient(get_secret_variables()[1])

def scheduled_job():
    geocoded_location = client.geocode("42370 Bob Hope Drive, Rancho Mirage CA")
    print(geocoded_location.coords)

sched = BlockingScheduler()
# change minutes=2 to days=1
sched.add_job(scheduled_job, 'interval', seconds=5, start_date='2020-07-19 22:10:00', timezone='US/Eastern')

sched.start()
