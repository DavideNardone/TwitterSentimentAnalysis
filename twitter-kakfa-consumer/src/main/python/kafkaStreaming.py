from __future__ import print_function


# import sys
# sys.path.append('/Users/davidenardone/PycharmProjects/TwitterSentimentAnalysis/twitter-kakfa-consumer/src/main/python')
import os
import ConfigParser
import pickle
import re
import datetime
import math
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from TweetPreProcessing import TweetPreProcessing
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from lightning import Lightning
from numpy import random, array
import numpy as np



def sortByValue(rdd):

    return rdd.sortBy(lambda x: x[1] ,ascending=False)



def word_features(rdd):


        # replicate frequencies words to create the dictionary from TF-IDF
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

        # print(tf)
        # rdd = rdd.keys()

        return rdd


# def test(lp):
#     #
#     # broadcastVar.value -=1
#     # print(broadcastVar.value)

# new_values is a list
# last_sum is an integer
# def updateFunc(new_values,last_sum):
#
#     # return new_values
#     return sum(new_values) + (last_sum or 0)

def updateFunction(newValues, runningCount):
    if runningCount is None:
       runningCount = 0
    return sum(newValues, runningCount)  # add the new values with the previous running count to get the new count

# def test(rdd):
#
#     print(rdd.toDense)



def plotStreaming(lp):

    # print(arg1)
    # print(type(lp))
    # pos = 0
    # neg = 0


    now = datetime.datetime.now()
    now2 = now.strftime('%H:%M:%S')

    # Convert string to list
    tn = now2.split(":")

    # print(now2)

    gt = lp[0]
    predicted = lp[1]

    hour = int(tn[0])
    min = int(tn[1])
    sec = int(tn[2])

    # print(hour)
    # print(min)
    # print(sec)

    #TODO: implement better !!!!
    val =  (hour * 3600) + (min * 60) + (sec)

    print('gt=' + str(gt) + ', predicted=' + str(predicted))
    if(int(gt)==0):
        if (int(gt) == int(predicted)):
            viz.append(np.array([1]))
        else:
            viz.append(np.array([0]))
    elif (int(gt)==4):
        if (int(gt) == int(predicted)):
            viz2.append(np.array([1]))
        else:
            viz2.append(np.array([0]))
    # if( (int(gt)==int(predicted)) and int(gt)==4):
    #     # print('classified wrongly')
    #     viz2.append(np.array([[1], [0]]))




# spark-submit --jars ~/workspace_spark/spark-1.6.2-bin-hadoop2.6/external/spark-streaming-kafka-assembly_2.10-1.6.2.jar --py-files modules/TweetPreProcessing.py,modules/Emoticons.py,modules/Acronyms.py ~/PycharmProjects/TwitterSentimentAnalysis/twitter-kakfa-consumer/src/main/python/kafkaStreaming.py
if __name__ == "__main__":

    global lgn
    global viz,viz2


    # setting visualization tool
    lgn = Lightning(host='http://localhost:3000/')
    # lgn.use_session('e590c719-219c-4381-85eb-41e65fa793d9')
    lgn.create_session('Tweets Classification using Naive Bayes')

    viz = lgn.linestreaming(
        np.array([0]), color = [0, 255, 0],
        description = 'Classification of POSITIVE tweets',
        xaxis = 'i-th tweet',
        yaxis = 'classification\'s value'
        )
    viz2 = lgn.linestreaming(
        np.array([0]),
        color = [255, 100, 100],
        description = 'Classification of NEGATIVE tweets',
        xaxis='i-th tweet',
        yaxis='classification\'s value'
    )


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

    # setting checkpoint
    # ssc.checkpoint(".")

    # Loading TF MODEL and compute TF-IDF
    print('Loading TRAINING_TF_MODEL...')
    tf_training = sc.pickleFile('/Users/davidenardone/Desktop/TF_MODEL')

    print('Computing TF-IDF MODEL...')
    global idf_training
    idf_training = IDF().fit(tf_training)
    # tfidf_training = idf_training.transform(tf_training)

    print('Loading Naive Bayes Model...')
    NBM = NaiveBayesModel.load(sc, "/Users/davidenardone/Desktop/NaiveBayesModel")

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

    tweet = lines.flatMap(obj1.TweetBuilder)

    label = tweet.map(lambda tup: tup[0]) \
        .transform(lambda x: x.zipWithUniqueId()) \
        .map(lambda line: (line[1], int(line[0])))

    # int() casting string 'label' to int

    hashingTF = HashingTF()

    #computing TF-IDF for each tweet and classifying it
    tf_tweet = tweet.map(lambda tup: hashingTF.transform(tup[1])) \
                    .transform(lambda tup: idf_training.transform(tup)) \
                    .map(lambda p: int(NBM.predict(p)))\
                    .transform(lambda p: p.zipWithUniqueId()) \
                    .map(lambda line: (line[1], line[0])) \
                    # .pprint()

    # Here the ground truth and the predicted class are joined
    # so, for each tweet we have the following structure:
    # (class_predicted, ground truth) i.e. (4,0),(0,0)

    cc = label.join(tf_tweet) \
            .map(lambda tup: tup[1]) \
            .map(plotStreaming) \
            .pprint()


            # .map(lambda tup: (tup,1)) \
            # .pprint()

    ssc.start()
    ssc.awaitTermination()



    # feature_words = tweets.flatMap(lambda x: x[0])
                    # .map(lambda word: (word, 1))\
                    # .updateStateByKey(updateFunc)\
                    # .transform(sortByValue) \
                    # .transform(word_features) \
                    # .pprint(30)


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



    ################################################TRAINING MODEL##########################################



    # sc = SparkContext(appName="TrainingBayesModel")
    # ssc = StreamingContext(sc, 1)
    #
    # allData = sc.textFile(
    #                     "/Users/davidenardone/twitterDataset/twitter/test_data.txt",
    #                       use_unicode=False
    #                      )
    #
    # obj1 = TweetPreProcessing()
    #
    # data = allData.map(lambda x: x.replace("\'",''))\
    #               .map(lambda x: x.split('",'))\
    #               .flatMap(obj1.TweetBuilder)
    #
    #
    # training, test = data.randomSplit([0.7, 0.3], seed=0)
    #
    # hashingTF = HashingTF()
    #
    # print('computing TF-IDF...')
    #
    # tf_training = training.map(lambda tup: hashingTF.transform(tup[1]))
    #
    # # tf_training.foreach(print)
    # # print('Saving TF_MODEL...')
    # # tf_training.saveAsPickleFile('/Users/davidenardone/Desktop/TF_MODEL')
    #
    # idf_training = IDF().fit(tf_training)
    #
    # tfidf_training = idf_training.transform(tf_training)
    #
    # tfidf_idx = tfidf_training.zipWithIndex()
    #
    # training_idx = training.zipWithIndex()
    #
    # idx_training = training_idx.map(lambda line: (line[1], line[0]))
    #
    # idx_tfidf = tfidf_idx.map(lambda l: (l[1], l[0]))
    #
    # joined_tfidf_training = idx_training.join(idx_tfidf)
    #
    # training_labeled = joined_tfidf_training.map(lambda tup: tup[1])
    #
    # # labeled_training_data.foreach(print)
    #
    # labeled_training_data = training_labeled.map(lambda k: LabeledPoint(k[0][0], k[1]))
    #
    # print('computing Naive Bayes Model...')
    #
    # model = NaiveBayes.train(labeled_training_data, 1.0)
    #
    # print('Saving Naive Bayes Model...')
    #
    # model.save(sc, "/Users/davidenardone/Desktop/NaiveBayesModel_2")
    #
    # tf_test = test.map(lambda tup: hashingTF.transform(tup[1]))
    #
    # idf_test = IDF().fit(tf_test)
    #
    # tfidf_test = idf_test.transform(tf_test)
    #
    # tfidf_idx = tfidf_test.zipWithIndex()
    #
    # test_idx = test.zipWithIndex()
    #
    # idx_test = test_idx.map(lambda line: (line[1], line[0]))
    #
    # idx_tfidf = tfidf_idx.map(lambda l: (l[1], l[0]))
    #
    # joined_tfidf_test = idx_test.join(idx_tfidf)
    #
    # test_labeled = joined_tfidf_test.map(lambda tup: tup[1])
    #
    # labeled_test_data = test_labeled.map(lambda k: LabeledPoint(k[0][0], k[1]))
    #
    # # labeled_training_data.foreach(lambda p: p.f)
    # # exit(-1)
    #
    # predictionAndLabel = labeled_test_data.map(lambda p: (model.predict(p.features), p.label))
    #
    # accuracy = 1.0 * predictionAndLabel.filter(lambda (x, v): x == v).count() / labeled_test_data.count()
    #
    # print(accuracy)
    #
    # labeled_2 = test_labeled.map(lambda k: (k[0][1], LabeledPoint(k[0][0], k[1])))
    #
    # predictionAndLabel2 = labeled_2.map(lambda p: [p[0], model.predict(p[1].features), p[1].label])
    #
    # accuracy = 1.0 * predictionAndLabel2.filter(lambda (x, v): x == v).count() / labeled_test_data.count()
    #
    # print(accuracy)

    ################################################TRAININING MODEL########################################