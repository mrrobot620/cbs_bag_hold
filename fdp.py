from selenium import webdriver
import os 
from select import select
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC3
from selenium.webdriver.common.action_chains import ActionChains
import time
import pandas as pd
import os
from idna import valid_contextj
from datetime import datetime, timedelta
import logging


logging.basicConfig(format='%(asctime)s %(message)s' , datefmt='%m/%d/%Y %I:%M:%S %p' , filename='auto_pendency.logs' , level=logging.DEBUG )

with open('fdp_links.txt' , 'r') as f:
    links = f.readlines()

print(links)

op = webdriver.ChromeOptions()
op.add_argument('--headless=new')
prefs = {
    'profile.default_content_settings.popups': 0,
    'download.default_directory' : r"/home/administrator/cbs_bag_hold",
    'directory_upgrade': True
}
op.add_experimental_option('prefs' , prefs)
driver = webdriver.Chrome(options=op)
driver.get("http://fdp.fkinternal.com/reports/view/scp/ekl/ykb_bagging_pending_automation_abhi")
time.sleep(5)
print(driver.title)



def login():
    username = driver.find_element(By.XPATH , "/html/body/div[2]/div[2]/div/div/form/div/div[4]/input[1]")
    username.send_keys("ca.2670054")

    password = driver.find_element(By.XPATH , "/html/body/div[2]/div[2]/div/div/form/div/div[4]/input[2]")
    password.send_keys("Veer@809")


    try:
        cross = driver.find_element(By.XPATH , "/html/body/div[4]/div/button")
        cross.click()
    except:
        print("Cross Button Failed")

    time.sleep(1)

    submit = driver.find_element(By.XPATH , "/html/body/div[2]/div[2]/div/div/form/div/div[4]/div[4]/button/span")
    submit.click()
    time.sleep(5)

def uiMover():
    no_thanks = driver.find_element(By.XPATH , "/html/body/div[3]/div/div/div/div[2]/button[1]")
    no_thanks.click()
    time.sleep(1)
    cross2 = driver.find_element(By.XPATH , "/html/body/div/div/div[1]/div[1]/div/div/div[2]/div[1]/span")
    cross2.click()
    time.sleep(1)

def primary():
    try:
        driver.refresh()
        time.sleep(4)
        primary1 = driver.find_element(By.XPATH ,"/html/body/div/div/div[2]/div[4]/div[2]/div/div[3]/div/div/div[2]/div/div/div/div[2]/div/div[2]/div/div/div[2]/div/div[4]/div/div/span/span").text
        # print(f"Current Primary number is: {primary1} ")
    except:
        print("Unable to print the Data")


def secondary():
    time.sleep(2)
    driver.get("http://fdp.fkinternal.com/reports/view/scp/ekl/YKB-Secondary-Sortation-Abhi")
    time.sleep(2)
    driver.refresh()
    time.sleep(2)
    try:
        secondary1 = driver.find_element(By.XPATH , "/html/body/div/div/div[2]/div[3]/div[2]/div/div[3]/div/div[1]/div[2]/div/div/div/div[2]/div/div[2]/div/div/div[2]/div/div[4]/div/div/span/span").text
        # print(F'The Secondary Number is: {secondary1}')
    except:
        print("Unable to get the secondary numbers")



def report_downloader(report_link):
    time.sleep(2)
    driver.get(report_link)
    time.sleep(1)
    try:
        download_button = driver.find_element(By.XPATH , "/html/body/div/div/div[2]/div[3]/div[2]/div/div[1]/div[3]/div/div/div/div/span[1]")
        download_button.click()
        time.sleep(1)
    except Exception as E:
        logging.warning(f"Error in fetching:   {report_link}: {E}")
    try:
        download_button1 = driver.find_element(By.XPATH , "/html/body/div/div/div[2]/div[3]/div[2]/div/div[1]/div[3]/div/div/div/ul/div/li[6]/div")
        download_button1.click()
        time.sleep(1)
    except Exception as E:
        logging.warning(f"Error in fetching:   {report_link} : {E}")
    try:
        report_download = driver.find_element(By.XPATH , "/html/body/div/div/div[2]/div[5]/div/div[2]/div/div[1]/div[1]/button")
        report_download.click()
        time.sleep(3)
        logging.warning(f"Fetched Sucessfully: {report_link}")
    except Exception as E:
        logging.warning(f"Error in fetching:   {report_link} : {E}")




driver.maximize_window()
login()
uiMover()
time.sleep(4)
for link in links:
    report_downloader(link)
driver.close()
