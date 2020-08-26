from apscheduler.schedulers.blocking import BlockingScheduler
import os
import geocodio

def get_secret_variables():
    if os.getenv("LOCAL_DEV") == "true":
        import secret_variables
        return secret_variables.DATABASE_URL, secret_variables.GEOCODIO_API_KEY
    return os.getenv("DATABASE_URL"), os.getenv("GEOCODIO_API_KEY")

client = geocodio.GeocodioClient(get_secret_variables()[1])
geocoded_location = client.geocode("42370 Bob Hope Drive, Rancho Mirage CA")
print(geocoded_location.coords)

# def scheduled_job():
#     geocoded_location = client.geocode("42370 Bob Hope Drive, Rancho Mirage CA")
#     print(geocoded_location.coords)
#
# sched = BlockingScheduler()
# # change minutes=2 to days=1
# sched.add_job(scheduled_job, 'interval', seconds=3, start_date='2020-07-19 22:10:00', timezone='US/Eastern')
#
# sched.start()
