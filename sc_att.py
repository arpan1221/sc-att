from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as cond
from selenium.webdriver.common.by import By
from time import sleep
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome import options
from PIL import Image
import os
import time
import urllib.request
import random_user_agent
import pandas as pd
import numpy as np
from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem
import sqlalchemy as db
from sqlalchemy import create_engine
import mysql.connector
from mysql.connector.constants import ClientFlag
import pymysql
from google.cloud import storage
import datetime
from datetime import datetime
day = datetime.today().strftime('%A')
print(day)
if day.lower()=='monday' or day.lower()=='thursday' or day.lower()=='saturday':
    software_names = [SoftwareName.CHROME.value]
    operating_systems = [OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value]  
    user_agent_rotator = UserAgent(software_names=software_names, operating_systems=operating_systems, limit=100)
    GOOGLE_CHROME_BIN = '/app/.apt/usr/bin/google-chrome'
    CHROMEDRIVER_PATH = '/app/.chromedriver/bin/chromedriver'
    def upload_blob(bucket_name, source_file_name, destination_blob_name):
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        x = datetime.now()
        x = x.strftime("%d-%m-%y")
        blob = bucket.blob("digital_captures/pdp/{}/att/".format(x)+destination_blob_name)
        blob.upload_from_filename(source_file_name)
        print(
            "File {} uploaded to {}.".format(
                source_file_name, destination_blob_name
            )
        )
    db_user='root'
    db_pass='root_password'
    db_name='odin_db'
    db_host="35.239.9.249:3306"
    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                        .format(user=db_user,
                                pw=db_pass,
                                db=db_name,
                                host=db_host))                                                   
    connection = engine.connect()
    metadata = db.MetaData()
    pricing_table = db.Table('product_and_promo', metadata, autoload=True, autoload_with=engine)
    query = db.select([db.func.max(pricing_table.columns.date_of_scrape)]).where(pricing_table.columns.source=='us_att')
    results = connection.execute(query).first()
    latest_date=None
    if results is not None:
        latest_date = results[0]

    print(latest_date.date())
    query = db.select([pricing_table]).where(pricing_table.columns.source=='us_att').where(pricing_table.columns.date_of_scrape>=latest_date.date())
    query_count = db.select([db.func.count(pricing_table.columns.id)]).where(pricing_table.columns.source=='us_att').where(pricing_table.columns.date_of_scrape>=latest_date.date())
    results = connection.execute(query).fetchall()
    results_count = connection.execute(query_count).scalar()
    results_df=pd.DataFrame();
    if results_count > 0:
        results_df = pd.DataFrame(results)
        print("Total rows returned - " + str(len(results_df)))
        results_df.columns = results[0].keys()
        results_df['product_name'] = results_df['product_name'].replace({np.nan: ''})                 
    results_df=results_df.sort_values(['product_url']).drop_duplicates(['product_url'], keep='first',inplace=False).reset_index(drop=True)
    print("Total unique links - " + str(len(results_df)))
    for i in range(len(results_df)):
        user_agent=user_agent_rotator.get_random_user_agent()
        options = webdriver.ChromeOptions()
        options.binary_location = GOOGLE_CHROME_BIN
        options.add_argument("--headless")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-gpu")
        driver = webdriver.Chrome(executable_path=CHROMEDRIVER_PATH,options=options)
        driver.implicitly_wait(10)
        driver.set_window_size(1920, 1100)
        print("Getting for product" + results_df['product_name'][i] + ", link - " + str(results_df['product_url'][i]))
        driver.get(results_df['product_url'][i])
        sleep(10)
        memory_elements=driver.find_elements_by_css_selector("#Capacity label");
        print("Found variants - " + str(len(memory_elements)))
        if len(memory_elements) > 0:
            counter=1;
            for elem in memory_elements:
                print("clicking on elem " + str(counter))
                driver.execute_script("arguments[0].click();",elem)
                sleep(3)
                filename=str(results_df['product_name'][i])+"-var-"+str(counter)+".png"
                s = driver.get_window_size()
                w = driver.execute_script('return document.body.parentNode.scrollWidth')
                h = driver.execute_script('return document.body.parentNode.scrollHeight')
                driver.set_window_size(w, h)
                driver.find_element_by_tag_name('body').screenshot(filename)
                upload_blob("odin-images", filename, filename)
                driver.set_window_size(s['width'], s['height'])
                counter=counter+1
        else:
            print("No variant, taking screenshot")
            filename=str(results_df['product_name'][i])+".png"
            s = driver.get_window_size()
            w = driver.execute_script('return document.body.parentNode.scrollWidth')
            h = driver.execute_script('return document.body.parentNode.scrollHeight')
            driver.set_window_size(w, h)
            driver.find_element_by_tag_name('body').screenshot(filename)
            upload_blob("odin-images", filename, filename)
            driver.set_window_size(s['width'], s['height'])
    driver.quit()
    print("end...")
else:
    print("Today is not a scraping day")



