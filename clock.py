from apscheduler.schedulers.blocking import BlockingScheduler
import pandas as pd
from sqlalchemy import create_engine
engine = create_engine('postgres://txmzafvlwebrcr:df20d17265cf81634b9f689187248524a6fd0d56222985e2f422c71887ec6ec0@ec2-34-224-229-81.compute-1.amazonaws.com:5432/dbs39jork6o07d')

def scheduled_job():
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    df.to_sql("new3", engine, if_exists='append', index=False)
    print("maybe this succeeded")

sched = BlockingScheduler()
# change minutes=2 to days=1
sched.add_job(scheduled_job, 'interval', minutes=2, start_date='2020-07-19 16:30:00', timezone='US/Eastern')

sched.start()
