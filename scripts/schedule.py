import time
from apscheduler.schedulers.blocking import BlockingScheduler
from update_database import update_database
from implement_fixes import send_fixes_to_postgres
from overwrite_arcgis import overwrite_our_feature
from update_status_columns import update_status_columns
from helpers import print_red_and_email
from colorama import Fore, Style

def perform_task_and_catch_errors(task_function, task_name):
    before = time.time()
    print(Fore.GREEN + f"{task_name}..." + Style.RESET_ALL)
    try:
        task_function()
    except Exception as error:
        print_red_and_email(str(error), f"Unanticipated Error {task_name.lower()}!!")
    print(Fore.GREEN + f"Finished {task_name} in {time.time() - before} seconds." + "\n" + Style.RESET_ALL)

def update_task():
    perform_task_and_catch_errors(update_database, "UPDATING DATABASE")
    perform_task_and_catch_errors(overwrite_our_feature, "OVERWRITING ARCGIS FEATURE")
    perform_task_and_catch_errors(update_status_columns, "UPDATING STATUS COLUMNS")

def implement_fixes_task():
    perform_task_and_catch_errors(send_fixes_to_postgres, "IMPLEMENTING FIXES")

# update database at 1:00 am EST every day, check for fixes every 6 hours
sched = BlockingScheduler()
sched.add_job(update_task, 'interval', days=1, start_date='2020-09-09 01:00:00', timezone='US/Eastern')
# sched.add_job(implement_fixes_task, 'interval', hours=6, start_date='2020-09-10 18:00:00', timezone='US/Eastern')
sched.start()
