from apscheduler.schedulers.blocking import BlockingScheduler
import pandas as pd
from sqlalchemy import create_engine
engine = create_engine('postgres://txmzafvlwebrcr:df20d17265cf81634b9f689187248524a6fd0d56222985e2f422c71887ec6ec0@ec2-34-224-229-81.compute-1.amazonaws.com:5432/dbs39jork6o07d')

def scheduled_job():
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    df.to_sql("new2", engine, if_exists='append', index=False)
    print("maybe this succeeded")

sched = BlockingScheduler()
sched.add_job(scheduled_job, 'cron', hour='16', minute='2')

# @sched.scheduled_job('interval', minutes=1)
# def timed_job():
#     df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
#     df.to_sql("new", engine, if_exists='append', index=False)
#     print("helloooooo")

# this works locally but not on heroku
# when wake up there should be 4 of them (6 rows)
# @sched.scheduled_job('cron', hour='15', minute='45')
# def schdeuled_job():
#     df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
#     df.to_sql("new2", engine, if_exists='append', index=False)
#     print("maybe this succeeded")


# @sched.scheduled_job('cron', day_of_week='mon-fri', hour=17)
# def scheduled_job():
#     print('This job is run every weekday at 5pm.')

# @sched.scheduled_job('cron', hour='9,11,16,17')
# def timed_job():
#     print ctime()

sched.start()
