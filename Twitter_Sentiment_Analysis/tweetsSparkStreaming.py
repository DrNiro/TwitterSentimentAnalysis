import json
import re
import boto3
import sys
import simplejson as json
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from textblob import TextBlob
from decimal import *


class TweetAnalyzer:
    """
        Class for tweets' text sentiment analysis in the speed layer.
    """
    def clean_tweet(self, tweet):
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())

    def analyze_sentiment(self, tweet):
        analysis = TextBlob(self.clean_tweet(tweet))
        return analysis.polarity


# send result from streaming DStream object to the DynamoDB result table.
def process_rdd(time, rdd):
    dynamodb = boto3.resource('dynamodb')
    batch_layer_table = dynamodb.Table('TweetsAlerts')

    print("----------- %s -----------" % str(time))
    try:
        collected = rdd.collect()
        for item in collected:
            add_tweet_to_table(batch_layer_table, item[1], item[0])
            print(item[1])
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)


# add a tweet as a row to a DB table with its sentiment score value.
def add_tweet_to_table(table, tweet, senti_score):
    table.put_item(Item=
        {
            "UserID": tweet['user']["id_str"],
            "Date_and_Time": tweet['created_at'],
            "Text": tweet['text'],
            "ScreenName": tweet['user']["screen_name"],
            "SentimentScore": Decimal(str(senti_score))
        }
    )


def main():
    # Task configuration and initialization.
    tweetsAnalyzer = TweetAnalyzer()
    topic = "streamtweets"
    brokerAddresses = "localhost:9092"
    batchTime = 10
    neg_thres = -0.5

    # Creating stream.
    spark = SparkSession.builder.appName("TweetsAnalyzer").getOrCreate()
    sc = spark.sparkContext
    ssc = StreamingContext(sc, batchTime)
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokerAddresses})

    # Analyze the streamed tweets
    tweetsRDD = kvs.map(lambda s: json.loads(str(s[1])))
    senti_score = tweetsRDD.map(lambda tweet: (tweetsAnalyzer.analyze_sentiment(tweet['text']), tweet))
    negative_scores = senti_score.filter(lambda score: score[0] < neg_thres)
    negative_scores.foreachRDD(process_rdd)

    # Start running the task.
    ssc.start()
    ssc.awaitTermination()


if __name__ == '__main__':
    main()
