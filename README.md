# Twitter Real-Time Sentiment Analysis

## Introduction

Perform real-time sentiment analysis on tweets using Spark and Kafka, while monitoring potentially harmful content. It identify harmful and abusive content and contribute to a safer online environment. The project integrates Kafka, Spark and AWS DB for efficient data processing and analysis, ensuring scalability and real-time insights.

## Architecture
To achieve a real-time pipeline the Lambda Architecture is implemented, consisted of three main layers.
1. Batch layer: Incoming data from the data source being saved and indexed in batches.
2. Serving layer: The serving layer receives batches from the batch layer on a predefined schedule in near-real-time to make them available for querying.
3. Speed layer: The speed layer narrow the gap between when the data is created and when it’s available for querying by indexing all of the data in the serving layer’s current indexing job.

### Data source:
The data used in this project is newly posted tweets extracted with Tweeter API (tweepy) in real-time.

### Quering
The data is indexed into an AWS DynamoDB NoSQL database.


## Features
* Real-time tweet analysis: The project leverages the Twitter API to fetch real-time tweets and performs sentiment analysis to identify harmful and abusive content.
* Sentiment analysis: Used TextBlob library for this purpose, and selected monitored corpus to take into consideration specific words.
* Kafka integration: Efficient data streaming and processing are achieved through Kafka, enabling scalable and fault-tolerant tweet analysis.
* Spark streaming: The Spark streaming job consumes tweets from Kafka, allowing for real-time analysis and efficient handling of high volumes of data.
* AWS database integration: The project saves the detected tweets and their analysis results in an AWS database for storage and further analysis.


## Installation
1. Clone the repository

`git clone https://github.com/DrNiro/TwitterSentimentAnalysis.git`

2. Install the required dependencies

`pip install -r requirements.txt`

3. Set up your Twitter API credentials in the twitter_credentials.py file. You will need to create a Twitter Developer account and obtain your API keys.

TWITTER_API_KEY=`<your-api-key>`

TWITTER_API_SECRET=`<your-api-secret>`

TWITTER_ACCESS_TOKEN=`<your-access-token>`

TWITTER_ACCESS_SECRET=`<your-access-secret>`


## Usage
1. Start Kafka producer:
python3 tweetsKafkaProducer.py > tweetsKafkaProducer.log &

2. Start Spark Streamer:
./spark-2.4.5-bin-hadoop2.7/bin/spark-submit tweetsSparkStreaming.py > tweetsSparkStreaming.log &

3. Start SparkBatch:
./spark-2.4.5-bin-hadoop2.7/bin/spark-submit --master spark://{your_spark_machine_address} tweetsSparkBatch.py > tweetsSparkBatch.log
(Please note that the EC2 used for this project is no longer running and the instance address of your spark machine should be replaced in tweetsSparkBatch.py file).



