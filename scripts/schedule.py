from apscheduler.schedulers.blocking import BlockingScheduler
from update_database import update_database
from implement_fixes import send_fixes_to_postgres
from overwrite_arcgis import overwrite_our_feature
from helpers import print_red_and_email
from colorama import Fore, Style

def perform_task_and_catch_errors(task_function, task_name):
    print(Fore.GREEN + f"{task_name}..." + Style.RESET_ALL)
    try:
        task_function()
    except Exception as error:
        print_red_and_email(str(error), f"Unanticipate Error {task_name}!!")
    print(Fore.GREEN + f"Finished {task_name}." + "\n" + Style.RESET_ALL)

def update_task():
    perform_task_and_catch_errors(update_database, "UPDATING DATABASE")
    perform_task_and_catch_errors(overwrite_our_feature, "OVERWRITING ARCGIS FEATURE")

def implement_fixes_task():
    perform_task_and_catch_errors(send_fixes_to_postgres, "IMPLEMENTING FIXES")

def check_in():
    print("don't worry, i'm still running...")

sched = BlockingScheduler()
# update database at 5:15 pm EST every day, check for fixes every 6 hours
sched.add_job(update_task, 'interval', days=1, start_date='2020-09-09 17:15:00', timezone='US/Eastern')
sched.add_job(implement_fixes_task, 'interval', hours=6, start_date='2020-09-10 18:00:00', timezone='US/Eastern')
sched.add_job(check_in, 'interval', hours=3, start_date='2020-09-08 00:30:00', timezone='US/Eastern')
sched.start()
