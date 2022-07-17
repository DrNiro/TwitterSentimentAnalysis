from tweepy import API
from tweepy import Cursor
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
import json
import multiprocessing
import boto3
import twitter_credentials


class TwitterClient:
    """
        This class is able to receive tweets by username
    """
    def __init__(self, twitter_user=None):
        self.auth = TwitterAuthenticator().authenticate_twitter_app()
        self.twitter_client = API(self.auth)

        self.twitter_user = twitter_user

    def get_twitter_client_api(self):
        return self.twitter_client

    def get_user_timeline_tweets(self, num_tweets):
        tweets = []
        for tweet in Cursor(self.twitter_client.user_timeline, id=self.twitter_user).items(num_tweets):
            tweets.append(tweet)
        return tweets

    def get_friend_list(self, num_friends):
        friend_list = []
        for friend in Cursor(self.twitter_client.friends, id=self.twitter_user).items(num_friends):
            friend_list.append(friend)
        return friend_list

    def get_followers_list(self, num_followers):
        followers_list = []
        for follower in Cursor(self.twitter_client.followers, id=self.twitter_user).items(num_followers):
            followers_list.append(follower)
        return followers_list

    def get_home_timeline_tweets(self, num_tweets):
        home_timeline_tweets = []
        for tweet in Cursor(self.twitter_client.home_timeline, id=self.twitter_user).items(num_tweets):
            home_timeline_tweets.append(tweet)
        return home_timeline_tweets


class TwitterAuthenticator:
    """
        This class used for an easy twitter authentication
    """
    def authenticate_twitter_app(self):
        auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
        auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
        return auth


class TwitterStreamer:
    """
        Class for Streaming and processing live tweets.
    """
    def __init__(self, startStreamEvent, resultQueue):
        self.twitter_authenticator = TwitterAuthenticator()
        self.listener = None
        self.startStreamEvent = startStreamEvent
        self.resultQueue = resultQueue

    def stream_tweets(self, fetched_tweets_filename, hash_tag_list, language):
        # This handles Twitter autehntiacation and the connection to the Twitter Streaming API.
        self.listener = TwitterListener(fetched_tweets_filename, self.startStreamEvent, self.resultQueue)
        auth = self.twitter_authenticator.authenticate_twitter_app()
        stream = Stream(auth, self.listener)
        stream.filter(languages=language, track=hash_tag_list)


class TwitterListener(StreamListener):
    """"
        This is a basic listener class.
        While receiving data with on_data function, we clean the tweets to contain only the data we need.
        Also, manages data flow between SparkStreaming and SparkBatch.
    """
    def __init__(self, fetched_tweets_filename, startStreamEvent, resultQueue):
        self.fetched_tweets_filename = fetched_tweets_filename
        self.startStreamEvent = startStreamEvent
        self.resultQueue = resultQueue
        self.container = []
        self.sending = False
        self.dynamodb = boto3.resource('dynamodb')
        self.batch_layer_table = self.dynamodb.Table('SparkBatchLayer')
        self.num_of_tweets_per_stream = 500

    def on_data(self, data):
        try:
            # Clean tweets
            t = json.loads(data)
            saved_data = {}
            saved_data['text'] = t['text']
            saved_data['created_at'] = t['created_at']
            saved_data['user'] = {}
            saved_data['user']["id_str"] = t['user']["id_str"]
            saved_data['user']["name"] = t['user']["name"]
            saved_data['user']["screen_name"] = t['user']["screen_name"]
            saved_data['user']["location"] = t['user']["location"]
            stringos = json.dumps(saved_data)

            # write tweets to a file for safekeeping
            with open(self.fetched_tweets_filename, 'a') as tf:
                # collecting tweets to stream
                if not self.sending:
                    if "RT @" not in stringos:
                        tf.write(stringos + "\n\n")
                        self.container.append(stringos)
                        if len(self.container) >= self.num_of_tweets_per_stream:
                            self.sending = True
                            self.resultQueue.put(self.container)
                            self.startStreamEvent.set()

                # If sending tweets to stream, collect tweets to DB for batch layer to use.
                elif self.sending:
                    # send tweets to DynamoDB for spark
                    tf.write("STOPPED STREAMING - INSERTING TWEET TO DYNAMODB" + "\n\n")
                    self.container = []
                    if "RT @" not in stringos:
                        self.batch_layer_table.put_item(Item=
                                                            {
                                                                "UserID": t['user']["id_str"],
                                                                "Tweet": stringos
                                                            }
                                                        )
                    if not self.startStreamEvent.is_set():
                        self.sending = False
            return True
        except BaseException as e:
            if str(e) == "'text'":
                print("There is no text")
            else:
                print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        if status == 420:
            # Returning False on_data method in case rate limit occurs.
            return False
        print(status)




