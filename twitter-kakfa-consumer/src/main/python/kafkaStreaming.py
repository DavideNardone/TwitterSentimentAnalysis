from __future__ import print_function


# import sys
# sys.path.append('/Users/davidenardone/PycharmProjects/TwitterSentimentAnalysis/twitter-kakfa-consumer/src/main/python')
import os
import ConfigParser
import re

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from TweetPreProcessing import TweetPreProcessing


# def stemming(text):
#
#     # TODO: Remove all URLs(e.g.www.xyz.com), hash tags(e.g.  # topic), targets (@username)
#     # TODO: Correct the spellings; sequence of repeated characters is to be handled
#
#     # Remove punctualization, Symbols and number
#     text = text.lower().strip()
#     text = re.sub("[^0-9a-zA-Z ]", '', text)
#
#     # Remove stop words
#     stop_words = [];
#     # test = 'not'
#     with open('/Users/davidenardone/PycharmProjects/TwitterSentimentAnalysis/twitter-kakfa-consumer/src/main/resources/stopwords.txt') as f:
#         for line in f:
#             stop_words.append(line.replace("\n", ""))
#
#     if text in stop_words:
#         text = text.replace(text, '')
#
#     return text



if __name__ == "__main__":

    # if len(sys.argv) != 3:
    #     print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
    #     exit(-1)

    # print(os.getcwd())
    # exit(-1)

    config = ConfigParser.ConfigParser()
    # print(os.getcwd())
    config.read('/Users/davidenardone/PycharmProjects/TwitterSentimentAnalysis/twitter-kakfa-consumer/conf/consumer.conf')

    #reading configuration
    app_name = config.get('Spark configurations', 'spark.app.name')
    spark_master = config.get('Spark configurations', 'spark.master')
    spark_batch_duration = config.get('Spark configurations', 'spark.batch.duration')

    kafka_topic = config.get('Kafka configurations', 'kafka.topics').replace('"', '''''')
    kafka_brokers = config.get('Kafka configurations', 'kafka.brokers').replace('"', '''''')

    # Create Spark context
    sc = SparkContext(
        appName = app_name,
        master  = spark_master
    )

    # Create Streaming context
    ssc = StreamingContext(
        sc,
        int(spark_batch_duration)
    )
    kafkaParams = {'metadata.broker.list"': kafka_brokers}

    # Create direct kafka stream with brokers and topics
    kvs = KafkaUtils.createDirectStream(
        ssc,
        [kafka_topic],
        {"metadata.broker.list": kafka_brokers}
    )

    obj1 = TweetPreProcessing()

    lines = kvs.map(lambda x: x[1])

    # stemming(lines).pprint()
    # tweets = lines.flatMap(lambda line: line.split(" "))\

    tweets = lines.map(obj1.TweetBuilder).pprint()
            # .map(lambda sentiment: (obj1.getSentiment(sentiment), 1)).pprint()
            # .reduceByKey(lambda a, b: a+b).pprint()
            # .countByValue().pprint()



        # .map(obj1.TweetBuilder).pprint()
    # tweets = lines.flatMap(lambda line: line.split(" ")[10:]).pprint()
    # stemmedTweets = tweets.map(obj1.stemming)\
    #     .filter(lambda x: x != '').pprint()


    # exit(-1)

    # counts = lines.flatMap(lambda line: line.split(" ")) \
    #     .map(lambda word: (word, 1)) \
    #     .reduceByKey(lambda a, b: a+b)
    # counts.pprint()
    ssc.start()
ssc.awaitTermination()