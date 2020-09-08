from apscheduler.schedulers.blocking import BlockingScheduler
from update_database import update_database
from implement_fixes import send_fixes_to_postgres
from helpers import print_red_and_email, myprint
from colorama import Fore, Style

print_red_and_email("hello", Fore.RED + "HELLO" + Style.RESET_ALL)
myprint("hi", is_red="red")
import pandas as pd
housing = pd.read_excel(os.path.join(os.getcwd(), '..', 'excel_files/housing_addendum.xlsx'))
print(housing)

exit()

def perform_task_and_catch_errors(task_function, task_name):
    print(Fore.GREEN + f"{task_name}..." + Style.RESET_ALL)
    try:
        task_function()
    except Exception as eroor:
        print_red_and_email(str(error), f"UNANTICIPATED ERROR {task_name}")
    print(Fore.GREEN + f"Finished {task_name}." + "\n" + Style.RESET_ALL)

def update_task():
    perform_task_and_catch_errors(update_database, "UPDATING DATABASE")

def implement_fixes_task():
    perform_task_and_catch_errors(send_fixes_to_postgres, "IMPLEMENTING FIXES")

sched = BlockingScheduler()
# update database at 5:15 pm EST every day, check for fixes every 6 hours
sched.add_job(update_task, 'interval', days=1, start_date='2020-09-08 17:15:00', timezone='US/Eastern')
sched.add_job(implement_fixes_task, 'interval', hours=6, start_date='2020-09-10 18:00:00', timezone='US/Eastern')
sched.start()
