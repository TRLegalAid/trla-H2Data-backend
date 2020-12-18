"""Script which runs all necessary tasks once per day. This is the sript which is permanently running on Heroku (when the dyno is turned on)."""

import time
from apscheduler.schedulers.blocking import BlockingScheduler
from helpers import print_red_and_email
from colorama import Fore, Style
from update_database import update_database
from implement_fixes import send_fixes_to_postgres
from overwrite_arcgis import overwrite_our_feature
from update_status_columns import update_status_columns_both_tables
from update_halfway_columns import update_halfway_columns
from mark_inactive_inaccurates_as_fixed import mark_all_inactive_low_accurates_as_fixed
from low_accuracies_google_sheet import send_fixes_in_our_google_sheet_to_low_accuracies, replace_our_google_sheet_with_low_accuracies_table
from fix_state_abbreviations import expand_abbreviations
from fix_previously_fixed import fix_previously_fixed

# performs the function task_funtion. task_name can be any string. if task_function results in an error, prints out the error message and emails it
# returns True if there was no error, else False
def perform_task_and_catch_errors(task_function, task_name):
    before = time.time()
    print(Fore.GREEN + f"{task_name}..." + Style.RESET_ALL)
    try:
        task_function()
        succeeded = True
    except Exception as error:
        print_red_and_email("Error: " + str(error), f"Unanticipated Error {task_name.lower()}!!")
        succeeded = False

    print(Fore.GREEN + f"Finished {task_name} in {time.time() - before} seconds." + "\n" + Style.RESET_ALL)
    return succeeded


def all_tasks():
    perform_task_and_catch_errors(update_database, "UPDATING DATABASE")
    perform_task_and_catch_errors(expand_abbreviations, "EXPANDING STATE ABBREVIATIONS")
    google_sheet_to_postgres_worked = perform_task_and_catch_errors(send_fixes_in_our_google_sheet_to_low_accuracies, "SENDING FIXES FROM GOOGLE SHEETS TO LOW ACCURACIES TABLE")

    # without this condition an error in the previous task would cause all fixes made in the google sheet to be lost
    if google_sheet_to_postgres_worked:
        perform_task_and_catch_errors(send_fixes_to_postgres, "IMPLEMENTING FIXES")
        perform_task_and_catch_errors(update_status_columns_both_tables, "UPDATING STATUS COLUMNS")
        perform_task_and_catch_errors(update_halfway_columns, "UPDATING HALFWAY COLUMNS")
        perform_task_and_catch_errors(mark_all_inactive_low_accurates_as_fixed, "MARKING INACTIVE INACCURATES AS FIXED")
        perform_task_and_catch_errors(fix_previously_fixed, "FIXING PREVIOUSLY FIXED")
        perform_task_and_catch_errors(send_fixes_to_postgres, "IMPLEMENTING FIXES")
        perform_task_and_catch_errors(replace_our_google_sheet_with_low_accuracies_table, "REPLACING OUR GOOGLE SHEET WITH LOW ACCURACIES TABLE")

    perform_task_and_catch_errors(overwrite_our_feature, "OVERWRITING ARCGIS FEATURE")


def perform_all_tasks():
    perform_task_and_catch_errors(all_tasks, "PERFORMING DAILY TASKS")

# performs all tasks at 1:00 am EST each day
sched = BlockingScheduler()
sched.add_job(perform_all_tasks, 'interval', days=1, start_date='2020-09-09 01:00:00', timezone='US/Eastern')
sched.start()
