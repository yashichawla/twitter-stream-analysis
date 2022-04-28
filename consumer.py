import os
import json
import logging
import datetime
import threading
from kafka import KafkaConsumer
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.ext.declarative import declarative_base

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - - %(filename)s - %(funcName)s - %(lineno)d -  %(message)s',
    level=logging.NOTSET,
    filename='consumer.log',
    filemode='w'
)

def setup_db():
    logging.info("Setting up database")
    query = "CREATE TABLE IF NOT EXISTS twitter(start_time timestamp, end_time timestamp, hashtag varchar not null, count int not null, primary key(start_time, end_time, hashtag));"
    connection.execute(query)
    return Table('twitter', metadata, autoload=True, autoload_with=engine)

def push_to_db(start_time, end_time, hashtag, count):
    logging.debug(f"Pushing to database values ({start_time}, {end_time}, {hashtag}, {count})")
    query = table.select().where(table.c.start_time == start_time).where(table.c.end_time == end_time).where(table.c.hashtag == hashtag)
    result = connection.execute(query).fetchall()
    if bool(result):
        logging.debug(f"Time period for hashtag exists in database. Updating count instead of inserting")
        count = count + result[0][3]
        query = table.update().where(table.c.start_time == start_time).where(table.c.end_time == end_time).where(table.c.hashtag == hashtag).values(count=count)
        connection.execute(query)
    else:
        logging.debug(f"Time period for hashtag does not exist in database. Inserting")
        query = table.insert().values(start_time=start_time, end_time=end_time, hashtag=hashtag, count=count)
        connection.execute(query)

def init_consumers():
    global kafka_consumers
    for hashtag in HASHTAGS:
        logging.info(f"Initializing consumer for {hashtag}")
        kafka_consumer = KafkaConsumer(  
            hashtag[1:],  
            bootstrap_servers = [f"{KAFKA_BROKER_HOSTNAME}:{KAFKA_BROKER_PORT}"],  
            auto_offset_reset = 'earliest',  
            enable_auto_commit = True,  
            value_deserializer = lambda x : json.loads(x.decode('utf-8'))  
        )  
        kafka_consumers[hashtag] = kafka_consumer

def init_threads():
    for hashtag in HASHTAGS:
        logging.info(f"Initializing thread for {hashtag}")
        thread = threading.Thread(target=consume, args=(hashtag,))
        thread.start()

def consume(hashtag):
    logging.info(f"Receiving data for {hashtag}")
    kafka_consumer = kafka_consumers[hashtag]
    while True:
        for message in kafka_consumer:
            logging.debug(message.value)
            start_time = datetime.datetime.strptime(message.value['start_time'], "%Y-%m-%d %H:%M:%S")
            end_time = datetime.datetime.strptime(message.value['end_time'], "%Y-%m-%d %H:%M:%S")
            count = message.value['count']
            push_to_db(start_time, end_time, hashtag, count)
            

if __name__ == "__main__":
    HASHTAGS = os.getenv("HASHTAGS").split()
    KAFKA_BROKER_HOSTNAME = os.getenv("KAFKA_BROKER_HOSTNAME")
    KAFKA_BROKER_PORT = int(os.getenv("KAFKA_BROKER_PORT"))
    DATABASE_HOSTNAME = os.getenv("DATABASE_HOSTNAME")
    DATABASE_PORT = int(os.getenv("DATABASE_PORT"))
    DATABASE_USERNAME = os.getenv("DATABASE_USERNAME")
    DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD")
    DATABASE_NAME = os.getenv("DATABASE_NAME")

    base = declarative_base()
    engine = create_engine(f'postgresql://{DATABASE_USERNAME}:{DATABASE_PASSWORD}@{DATABASE_HOSTNAME}:{DATABASE_PORT}/{DATABASE_NAME}')
    metadata = MetaData()
    connection = engine.connect()
    session = sessionmaker(bind=engine)()

    table = setup_db()
    kafka_consumers = dict()
    init_consumers()
    init_threads()