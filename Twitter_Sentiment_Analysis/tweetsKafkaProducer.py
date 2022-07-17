import multiprocessing
from kafka import KafkaProducer
import tweets_streamer as streamer

"""
    The Kafka producer - sending tweets to the SparkStreaming
"""
# publish content to a selected topic
def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print("Message '{0}' published successfully.".format(value))
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


# connect producer to topic
def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


# main function to manage the data streaming
def main():
    # start kafka producer
    kafka_producer = connect_kafka_producer()

    # name of the file to write raw tweets to.
    fetched_tweets_filename = 'tweets.txt'
    # words corpus to filter tweets by
    negs = open("./corpus.txt", "r").read().split("\n")[1:]

    # multiprocessing preparations
    queueResult = multiprocessing.Queue()
    startStreamEvent = multiprocessing.Event()

    # streamer object that start receiving tweets from the API
    tweets_streamer = streamer.TwitterStreamer(startStreamEvent, queueResult)

    # start the actual tweets stream in a new process to allow the main process to run in parallel
    parallel_stream = multiprocessing.Process(target=tweets_streamer.stream_tweets, args=[fetched_tweets_filename, negs, ["en"]])
    parallel_stream.start()

    # When the event is triggered from the tweets_streamer object, it starts to produce them into the Streaming consumer.
    while True:
        startStreamEvent.wait()
        if startStreamEvent.is_set():
            tweets_list = queueResult.get()
            for tweet in tweets_list:
                publish_message(kafka_producer, 'streamtweets', 'tweets', tweet)

            startStreamEvent.clear()


if __name__ == '__main__':
    main()
