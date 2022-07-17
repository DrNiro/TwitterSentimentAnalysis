import json
import re
import time

from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
import boto3
from decimal import *

from pyspark import SparkConf, SparkContext

from textblob import TextBlob


class BatchTableManager():
    """
        This class manage the incoming tweets that needs to be analyzed by the Batch-layer(Spark).
        Responsible on extracting and removing the tweets from this 'queue' table.
    """
    def __init__(self):
        self.dynamoDB_resource = boto3.resource('dynamodb')
        self.dynamoDB_client = boto3.client('dynamodb')
        self.batchTable = self.dynamoDB_resource.Table('SparkBatchLayer')

    def get_items_count(self):
        return self.batchTable.item_count

    def get_item(self):
        return self.dynamoDB_client.scan(TableName='SparkBatchLayer', Limit=1)

    def delete(self, attr1):
        try:
            self.batchTable.delete_item(
                Key={
                    'UserID': attr1
                }
            )
        except ClientError as e:
            if e.response['Error']['Code'] == "ConditionalCheckFailedException":
                print(e.response['Error']['Message'])
            else:
                raise


class TweetAnalyzer:
    """
        Class for tweets' text sentiment analysis for the batch layer.
    """
    # clean tweets from tags, hashtags and redundant spaces.
    def clean_tweet(self, tweet):
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())

    # sentiment analysis using textblob
    def analyze_sentiment(self, tweet):
        analysis = TextBlob(self.clean_tweet(tweet))
        return analysis.polarity


# function to add a tweet as a row to a DB table with its sentiment score value.
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


if __name__ == '__main__':
    # open a DynamoDB connection and receive the results table.
    dynamodb = boto3.resource('dynamodb')
    batch_layer_table = dynamodb.Table('TweetsAlerts')

    # Setup SparkContext object with configuration to the AWS EC2 machine.
    conf = SparkConf().setAppName('BatchAnalysis').setMaster('spark://ip-172-31-8-1.eu-west-2.compute.internal:7077')
    sc = SparkContext(conf=conf)

    # initialize the batch table manager and the tweets analyzer objects.
    table_manager = BatchTableManager()
    tweetsAnalyzer = TweetAnalyzer()
    # N is the batch size
    N = 15
    # sentiment score thresholds
    neg_thres = -0.5

    # loop always running and checking if there are tweets waiting for analysis in the batch layer
    batch = []
    while True:
        item = None
        try:
            item = table_manager.get_item()
            if len(item['Items']) == 0:
                print("no tweets")
                time.sleep(5)
            else:
                batch.append(item['Items'][0]['Tweet']['S'])
                userID = item['Items'][0]['UserID']['S']
                table_manager.delete(userID)
        except ClientError as e:
            print("Couldn't get item")

        # if collected batch-size tweets, start analysis. start to collect more on finish.
        if len(batch) == N:
            """
                SPARK BATCH ANALYSIS
            """
            tweets = [json.loads(js_tweet) for js_tweet in batch]
            tweetsRDD = sc.parallelize(tweets)
            senti_score = tweetsRDD.map(lambda tweet: (tweetsAnalyzer.analyze_sentiment(tweet['text']), tweet))
            negative_scores = senti_score.filter(lambda score: score[0] < neg_thres).collect()
            for it in negative_scores:
                add_tweet_to_table(batch_layer_table, it[1], it[0])
                print(it[1])

            batch = []





