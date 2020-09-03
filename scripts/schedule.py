from apscheduler.schedulers.blocking import BlockingScheduler
import os
import pandas as pd
from sqlalchemy import create_engine
import geocodio





# def scheduled_job():
#     print("made it into the scheduled job script...")
def job():
    for i in range(5):
        print(i)
    print("all done")


# sched = BlockingScheduler()
# # change minutes=2 to days=1
# sched.add_job(job, 'interval', seconds=5, start_date='2020-08-25 22:10:00', timezone='US/Eastern')
# # sched.add_job(fix, 'interval', days=1)
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
