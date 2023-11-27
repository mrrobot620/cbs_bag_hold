import subprocess
import schedule
import time
import threading
import logging
import math

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', filename='auto_pendency.logs', level=logging.DEBUG)

downloader = "fdp.py"
data_processor = "auto_pendency.py"

def run_tasks():
    try:
        subprocess.run(['python3', downloader])
        subprocess.run(['python3', data_processor])
    except Exception as E:
        print(E)
        logging.warning(E)

def schedule_tasks():
    threading.Thread(target=run_tasks).start()


def display_timer(duration):
    duration = int(math.ceil(duration)) 
    for remaining in range(duration, 0, -1):
        print(f"Next task in {remaining} seconds", end='\r')
        time.sleep(1)


run_tasks()

schedule.every(5).minutes.do(schedule_tasks)

while True:
    next_run = schedule.idle_seconds()
    display_timer(next_run)
    schedule.run_pending()
