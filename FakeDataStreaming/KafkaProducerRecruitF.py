from datetime import datetime, timedelta
from kafka import KafkaProducer
import time
import cassandra
import random
import pandas as pd
pd.set_option("display.max_rows", None, "display.max_columns", None)
import datetime
import pandas as pd
import mysql.connector
import json

from dotenv import load_dotenv
import os
load_dotenv(dotenv_path='../.env') 
USERNAME = os.getenv('MYSQL_USER')
PASSWORK = os.getenv('MYSQL_PASSWORD')
HOST= os.getenv('HOST')
DB_NAME= os.getenv('DB_NAME')
KAFKA_HOST_IP="kafka"
TOPIC='Recruit'

# Messages will be serialized as JSON 
def serializer(message):
    return json.dumps(message).encode('utf-8')

kafka_p = KafkaProducer(
    bootstrap_servers = [f'{KAFKA_HOST_IP}:9092'],  
    value_serializer=serializer
)

def get_data_from_job():
    cnx = mysql.connector.connect(user=USERNAME, password=PASSWORK,
                                         host=HOST,
                                      database=DB_NAME)
    query = """select id as job_id,campaign_id , group_id , company_id from job"""
    mysql_data = pd.read_sql(query,cnx)
    return mysql_data

def get_data_from_publisher():
    cnx = mysql.connector.connect(user=USERNAME, password=PASSWORK,
                                         host=HOST,
                                      database=DB_NAME)
    query = """select distinct(id) as publisher_id from master_publisher"""
    mysql_data = pd.read_sql(query,cnx)
    return mysql_data


def generating_dummy_data(n_records):
    log_list= list()
    publisher = get_data_from_publisher()
    publisher = publisher['publisher_id'].to_list()
    jobs_data = get_data_from_job()
    job_list = jobs_data['job_id'].to_list()
    campaign_list = jobs_data['campaign_id'].to_list()
    group_list = jobs_data[jobs_data['group_id'].notnull()]['group_id'].astype(int).to_list()
    for i in range(n_records):
        create_time = str(cassandra.util.uuid_from_time(datetime.datetime.now()))
        bid = random.randint(0,1)
        interact = ['click','conversion','qualified','unqualified']
        custom_track = random.choices(interact,weights=(70,10,10,10))[0]
        job_id = random.choice(job_list)
        publisher_id = random.choice(publisher)
        group_id = random.choice(group_list)
        campaign_id = random.choice(campaign_list)
        ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_list.append({
            "create_time": create_time,
            "bid":bid,
            "campaign_id":campaign_id,
            "custom_track":custom_track,
            "group_id":group_id,
            "job_id":job_id,
            "publisher_id":publisher_id,
            "ts":ts,
        })
    return log_list

def send_message():
    dummy_message= generating_dummy_data(n_records = random.randint(1,5))
    for item in dummy_message:
        print(f'Producing message @ {datetime.now()} | Message = {str(item)}')
        kafka_p.send(TOPIC, item)

while True:
    generating_dummy_data(n_records = random.randint(1,5))
    time.sleep(1)

