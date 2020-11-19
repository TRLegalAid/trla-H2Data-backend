import time
from apscheduler.schedulers.blocking import BlockingScheduler
from update_database import update_database
from implement_fixes import send_fixes_to_postgres
from overwrite_arcgis import overwrite_our_feature
from update_status_columns import update_status_columns_both_tables
from mark_inactive_inaccurates_as_fixed import mark_all_inactive_low_accurates_as_fixed
from low_accuracies_google_sheet import send_fixes_in_our_google_sheet_to_low_accuracies, replace_our_google_sheet_with_low_accuracies_table
from helpers import print_red_and_email
from colorama import Fore, Style

def perform_task_and_catch_errors(task_function, task_name):
    before = time.time()
    print(Fore.GREEN + f"{task_name}..." + Style.RESET_ALL)
    try:
        task_function()
    except Exception as error:
        print_red_and_email("Error: " + str(error), f"Unanticipated Error {task_name.lower()}!!")
    print(Fore.GREEN + f"Finished {task_name} in {time.time() - before} seconds." + "\n" + Style.RESET_ALL)


def all_tasks():
    perform_task_and_catch_errors(update_database, "UPDATING DATABASE")
    perform_task_and_catch_errors(update_status_columns_both_tables, "UPDATING STATUS COLUMNS")
    perform_task_and_catch_errors(mark_all_inactive_low_accurates_as_fixed, "MARKING INACTIVE INACCURATES AS FIXED")
    perform_task_and_catch_errors(send_fixes_in_our_google_sheet_to_low_accuracies, "SENDING FIXES FROM GOOGLE SHEETS TO LOW ACCURACIES TABLE")
    perform_task_and_catch_errors(send_fixes_to_postgres, "IMPLEMENTING FIXES")
    perform_task_and_catch_errors(replace_our_google_sheet_with_low_accuracies_table, "REPLACING OUR GOOGLE SHEET WITH LOW ACCURACIES TABLE")
    perform_task_and_catch_errors(overwrite_our_feature, "OVERWRITING ARCGIS FEATURE")

def perform_all_tasks():
    perform_task_and_catch_errors(all_tasks, "PERFORMING DAILY TASKS")

# update database at 1:00 am EST every day, check for fixes every 6 hours
sched = BlockingScheduler()
sched.add_job(perform_all_tasks, 'interval', days=1, start_date='2020-09-09 01:00:00', timezone='US/Eastern')
sched.start()
