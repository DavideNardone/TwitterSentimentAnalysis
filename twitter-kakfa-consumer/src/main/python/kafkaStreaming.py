from __future__ import print_function

import os
import ConfigParser
import re

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from TweetPreProcessing import TweeTPreProcessing




def stemming(text):

    # Remove punctualization, Symbols and number
    text = text.lower().strip()
    text = re.sub("[^0-9a-zA-Z ]", '', text)

    # Remove stop words
    stop_words = [];
    # test = 'not'
    with open('/Users/davidenardone/PycharmProjects/TwitterSentimentAnalysis/twitter-kakfa-consumer/src/main/resources/stopwords.txt') as f:
        for line in f:
            stop_words.append(line.replace("\n", ""))

    if text in stop_words:
        text = text.replace(text, '')

    return text



if __name__ == "__main__":

    # if len(sys.argv) != 3:
    #     print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
    #     exit(-1)

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

    # obj1 = TweeTPreProcessing()


    lines = kvs.map(lambda x: x[1])
    # stemming(lines).pprint()
    tweets = lines.flatMap(lambda line: line.split(" ")[10:])
    stemmedTweets = tweets.map(stemming).pprint()



    # counts = lines.flatMap(lambda line: line.split(" ")) \
    #     .map(lambda word: (word, 1)) \
    #     .reduceByKey(lambda a, b: a+b)
    # counts.pprint()
    ssc.start()
ssc.awaitTermination()