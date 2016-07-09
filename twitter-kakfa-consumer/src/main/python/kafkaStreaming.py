from __future__ import print_function

import os
import ConfigParser
import re
import unicodedata

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from Emoticons import Emoticons



class TweetPreProcessing:

    # sentiment = ''

    # def __init__(self):

    def stemming(self, text):

        # Convert list to string
        text = ' '.join(text)

        # TODO: Replace all the emoticons with their sentiment.

        dictionary = Emoticons().getDictionary()

        # print(dictionary[':)'])


        self.cleanUpTweet(text)

        # TODO: Correct the spellings; sequence of repeated characters is to be handled
        # TODO: Expand Acronyms(we can use a acronym dictionary)
        # TODO: Remove Non - English Tweets

        # Create stop words
        # TODO: Create class for it
        stop_words = [];
        with open(os.getcwd()+'/PycharmProjects/TwitterSentimentAnalysis/twitter-kakfa-consumer/src/main/resources/stopwords.txt') as f:
            for line in f:
                stop_words.append(line.replace("\n", ""))

        # Remove duplicate in tweet (e.g. Fair enough. But i have the Kindle2 and i think it's perfect)
        text_set = set(text.split())
        text_list = list(text_set)

        for ws in stop_words:
            if ws in text_list:
                text_list.remove(ws)

        return text_list


    def cleanUpTweet(self, text):
        # Remove punctuations, Symbols and number AND
        # all URLs(e.g.www.xyz.com), hash tags(e.g.  # topic), targets (@username)
        text = text.lower().strip()
        text = re.sub(" \d+", " ", text)  # Remove digit
        text = re.sub('(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)', '', text)

        return text


    def extractSentiment(self, text):
        sentiment = text[0]

        return sentiment



    def extractTweet(self, text):
            tweet = text[10:]

            return tweet


        # print(type(str(sentiment)))
        # tweet = tmp[10:].append(str(sentiment))
        # tweet = sentiment + " " + ', '.join(tmp[10:])
        # print(tweet)
        # return tweet.split(" ")


    def TweetBuilder(self, text):

        #convert unicode to string
        text_list = str(text).split()

        # print(type(text_list))
        # print(text_list)

        sentiment = self.extractSentiment(text_list)
        # print(sentiment)
        tweet = self.extractTweet(text_list)
        # print(tweet)
        tweet_stemmed = self.stemming(tweet)

        tweet_stemmed.insert(0,sentiment)

        tweet_stemmed = ' '.join(tweet_stemmed).split(" ")
        return tweet_stemmed

        # return tweet_stemmed



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
    tweets = lines.flatMap(obj1.TweetBuilder).pprint()
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