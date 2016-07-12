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
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint

from random import randint


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


def sortByValue(rdd):

    return rdd.sortBy(lambda x: x[1] ,ascending=False)


def word_features(rdd):


        # replicate frequencies words to create the dictioanry fro TF-IDF
        ws = []
        for x in rdd.toLocalIterator():
            word = x[0]
            freq = int(x[1])
            for i in range(freq):
                ws.append(word)

        # print(ws)

        # creating feature vector from ws
        htf = HashingTF(100)
        tf = htf.transform(ws)


        print(tf)


        # rdd = rdd.keys()

        return rdd


# new_values is a list
# last_sum is an integer
def updateFunc(new_values,last_sum):

    # return new_values
    return sum(new_values) + (last_sum or 0)


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
    # TODO: check whether is possible to set other variables such as: pyFiles, jars, ecc
    sc = SparkContext(
        appName = app_name,
        master  = spark_master
    )


    # Create Streaming context
    ssc = StreamingContext(
        sc,
        int(spark_batch_duration)
    )

    #setting checkpoint
    ssc.checkpoint("checkpoint")



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

    tweets = lines.flatMap(obj1.TweetBuilder)
    feature_words = tweets.flatMap(lambda x: x[0])\
                    .map(lambda word: (word, 1))\
                    .updateStateByKey(updateFunc)\
                    .transform(sortByValue) \
                    .transform(word_features) \
                    .pprint(30)



    # tweets = lines.flatMap(obj1.TweetBuilder)
    # .map(lambda word: (word, 1)) \
    #     .map(lambda (k, v): (tuple(k), v)) \
    #     .pprint()
    # .reduceByKey(lambda x,y: x+y)\
    # .updateStateByKey(updateFunc)\
    # .pprint()
    # .transform(word_features)\
    # .pprint()
    # .transform(sortByValue)\
    # .pprint()


    ssc.start()
ssc.awaitTermination()