from apscheduler.schedulers.blocking import BlockingScheduler
import os
import pandas as pd
from sqlalchemy import create_engine
import geocodio


# def scheduled_job():
#     print("made it into the scheduled job script...")
for i in range(10):
    print("3")
#
# sched = BlockingScheduler()
# # change minutes=2 to days=1
# sched.add_job(update, 'interval', days=1, start_date='2020-08-25 22:10:00', timezone='US/Eastern')
# sched.add_job(fix, 'interval', days=1)
#
# sched.start()

# for checking for changes in low_accuracies
# def fix():
#     old_df = pd.from_sql(low_accuracies)
#     while True:
#         new_df = pd.from_sql(low_accuracies)
#         if not (new_df == old_df).all().all():
#             run_implement_fixes()
#         old_df = new_df
