import os
import json
import logging
import datetime
from kafka import KafkaProducer
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import window
from pyspark.streaming import StreamingContext

from dotenv import load_dotenv
load_dotenv()

# logging.basicConfig(
#     format='%(asctime)s - %(name)s - %(levelname)s - - %(filename)s - %(funcName)s - %(lineno)d -  %(message)s',
#     level=logging.DEBUG,
#     filename='producer.log',
#     filemode='w'
# )

def get_hashtag_dataframes(df):
    dfs = {hashtag:df.filter(df.tweet.contains(hashtag)) for hashtag in HASHTAGS}
    return dfs

def get_tumbling_window_dataframes(hashtag_dfs):
    windows = dict()
    for hashtag, hashtag_df in hashtag_dfs.items():
        w = hashtag_df.groupBy(window(timeColumn="time", windowDuration="5 minutes")).count().alias("count")
        windows[hashtag] = w
    return windows

def send_windows_to_consumer(tuming_window_dfs):
    for hashtag, window in tuming_window_dfs.items():
        for row in window.collect():
            data = {
                "hashtag": hashtag,
                "start_time": row.window.start.strftime("%Y-%m-%d %H:%M:%S"),
                "end_time": row.window.end.strftime("%Y-%m-%d %H:%M:%S"),
                "count": row["count"]
            }
            print(data)
            kafka_producer.send(hashtag[1:], value=data)

def process_stream(tweets):
    tweets = tweets.collect()
    if not tweets:
        return
    
    tweets = [json.loads(tweet) for tweet in tweets]
    result = list()
    for tweet in tweets:
        tweet_time = datetime.datetime.strptime(tweet["time"], "%a %b %d %H:%M:%S %z %Y")
        result.append((tweet_time, tweet["tweet"]))

    df = spark_sql_context.createDataFrame(result, ["time", "tweet"]).toDF("time", "tweet")
    hashtag_dfs = get_hashtag_dataframes(df)
    tumbling_window_dfs = get_tumbling_window_dataframes(hashtag_dfs)
    send_windows_to_consumer(tumbling_window_dfs)

if __name__ == "__main__":
    HASHTAGS = os.getenv("HASHTAGS").split()
    STREAM_HOSTNAME = os.getenv("STREAM_HOSTNAME")
    STREAM_PORT = int(os.getenv("STREAM_PORT"))
    KAFKA_BROKER_HOSTNAME = os.getenv("KAFKA_BROKER_HOSTNAME")
    KAFKA_BROKER_PORT = int(os.getenv("KAFKA_BROKER_PORT"))

    spark_context = SparkContext.getOrCreate()
    spark_streaming_context = StreamingContext(spark_context, 5)
    spark_sql_context = SQLContext(spark_context)
    kafka_producer = KafkaProducer(
        bootstrap_servers=f"{KAFKA_BROKER_HOSTNAME}:{KAFKA_BROKER_PORT}",
        value_serializer = lambda x:json.dumps(x).encode('utf-8')
    )

    spark_streaming_context\
        .socketTextStream(STREAM_HOSTNAME, STREAM_PORT)\
        .foreachRDD(process_stream)
    
    spark_streaming_context.start()
    spark_streaming_context.awaitTermination(1000)
    spark_streaming_context.stop()