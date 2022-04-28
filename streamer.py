import os
import json 
import socket
import tweepy 
import logging
from tweepy import Stream

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - - %(filename)s - %(funcName)s - %(lineno)d -  %(message)s',
    level=logging.NOTSET,
    filename='stream.log',
    filemode='w'
)

class TwitterListener(Stream):
    def on_data(self, data):
        try:
            tweet_content = json.loads(data)["text"]
            tweet_time = json.loads(data)["created_at"]
            data = {
                "tweet": tweet_content,
                "time": tweet_time
            }
            logging.debug(data)
            client_socket.send((json.dumps(data) + "\n").encode())
            return True

        except BaseException as e:
            logging.error(f"Error parsing data: {e}")
            return True

    def on_error(self, status):
        logging.error(status)

class TwitterStreamer():
    def __init__(self, client_socket):
        self.CONSUMER_KEY = os.environ.get('CONSUMER_KEY')
        self.CONSUMER_SECRET = os.environ.get('CONSUMER_SECRET')
        self.ACCESS_TOKEN = os.environ.get('ACCESS_TOKEN')
        self.ACCESS_SECRET = os.environ.get('ACCESS_SECRET')
        self.client_socket = client_socket
        self.auth = tweepy.OAuthHandler(self.CONSUMER_KEY, self.CONSUMER_SECRET)
        self.auth.set_access_token(self.ACCESS_TOKEN, self.ACCESS_SECRET)
        self.api = tweepy.API(self.auth)

    def stream_tweets(self, hashtags):
        stream = TwitterListener(self.CONSUMER_KEY, self.CONSUMER_SECRET, self.ACCESS_TOKEN, self.ACCESS_SECRET)
        stream.filter(track=hashtags, threaded=True)

if __name__ == "__main__":
    HASHTAGS = os.getenv("HASHTAGS").split()
    STREAM_HOSTNAME = os.getenv("STREAM_HOSTNAME")
    STREAM_PORT = int(os.getenv("STREAM_PORT"))
    STREAM_SOCKET = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    STREAM_SOCKET.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    STREAM_SOCKET.bind((STREAM_HOSTNAME, STREAM_PORT))
    STREAM_SOCKET.listen(1)
    logging.info(f"Listening for connections on {STREAM_HOSTNAME}:{STREAM_PORT}")

    client_socket, client_address = STREAM_SOCKET.accept()
    logging.info(f"Connection from {client_address} has been established!")
    twitter_streamer = TwitterStreamer(client_socket)
    twitter_streamer.stream_tweets(HASHTAGS)