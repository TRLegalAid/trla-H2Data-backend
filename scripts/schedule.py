import time
from apscheduler.schedulers.blocking import BlockingScheduler
from update_database import update_database
from implement_fixes import send_fixes_to_postgres
from overwrite_arcgis import overwrite_our_feature
from check_for_duplicates import check_for_duplicates
from backup_database import backup_database_on_postgres
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
    perform_task_and_catch_errors(backup_database_on_postgres, "BACKING UP DATABASE")
    perform_task_and_catch_errors(update_database, "UPDATING DATABASE")
    perform_task_and_catch_errors(overwrite_our_feature, "OVERWRITING ARCGIS FEATURE")

def implement_fixes_task():
    perform_task_and_catch_errors(backup_database_on_postgres, "BACKING UP DATABASE")
    perform_task_and_catch_errors(send_fixes_to_postgres, "IMPLEMENTING FIXES")

def check_for_duplicates_task():
    perform_task_and_catch_errors(check_for_duplicates, "CHECKING FOR DUPLICATES")

def check_in():
    print("don't worry, i'm still running...")

# update database at 5:15 pm EST every day, check for fixes every 6 hours, check for duplicates at 10:45 pm every day
sched = BlockingScheduler()
sched.add_job(update_task, 'interval', days=1, start_date='2020-09-09 17:15:00', timezone='US/Eastern')
sched.add_job(implement_fixes_task, 'interval', hours=6, start_date='2020-09-10 18:00:00', timezone='US/Eastern')
# sched.add_job(check_for_duplicates_task, 'interval', days=1, start_date='2020-09-08 22:45:00', timezone='US/Eastern')
sched.add_job(check_in, 'interval', hours=3, start_date='2020-09-08 00:30:00', timezone='US/Eastern')
# sched.add_job(backup_database_task, 'interval', minutes=2, start_date='2020-10-03 11:35:00', timezone='US/Eastern')

sched.start()
