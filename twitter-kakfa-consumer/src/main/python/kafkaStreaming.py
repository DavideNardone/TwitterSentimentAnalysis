from __future__ import print_function

import ConfigParser
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from TweetPreProcessing import TweetPreProcessing
from pyspark.mllib.classification import NaiveBayesModel
from pyspark.sql import Row
import datetime
import os
import json
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF
from pyspark.sql.types import *



def decodeUnicode(text):

    text = json.loads(text.decode('ascii'))

    # turn unicode list elements into string element
    for word in text:
        loc = text.index(word)
        text[loc] = str(word)

    return text

# Lazily instantiated global instance of SQLContext
def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

def jdbcInsert(rdd):

    #  Get the singleton instance of SQLContext
    sqlContext = getSqlContextInstance(rdd.context)

    # Convert RDD[String] to RDD[Row] to DataFrame
    rowRdd = rdd.map(lambda w: Row(gt=w[0], predicted=w[1]))

    if(rowRdd.isEmpty() != True):
        DataFrame = sqlContext.createDataFrame(rowRdd)
        DataFrame.write.jdbc(MYSQL_CONNECTION_URL.value, tableName.value, mode = 'append')
    else:
        return

def createUniqueTableName(name):

    now = datetime.datetime.now().strftime('%d:%H:%M:%S')

    # Convert string to list
    tn = now.split(":")

    day = tn[0]
    hour = tn[1]
    min = tn[2]
    sec = tn[3]

    # table name i.e k_means_24_1_21_20
    table_name = name+'_'+day+'_'+hour+'_'+min+'_'+sec

    return table_name



# spark-submit --jars ~/workspace_spark/spark-1.6.2-bin-hadoop2.6/external/spark-streaming-kafka-assembly_2.10-1.6.2.jar --py-files modules/TweetPreProcessing.py,modules/Emoticons.py,modules/Acronyms.py ~/PycharmProjects/TwitterSentimentAnalysis/twitter-kakfa-consumer/src/main/python/kafkaStreaming.py
if __name__ == "__main__":

    config = ConfigParser.ConfigParser()
    wd = os.getcwd()
    config.read(wd+'/PycharmProjects/TwitterSentimentAnalysis/twitter-kakfa-consumer/conf/consumer.conf')

    #READING CONFIGURATION
    app_name = config.get('Spark configurations', 'spark.app.name')
    spark_master = config.get('Spark configurations', 'spark.master')
    spark_batch_duration = config.get('Spark configurations', 'spark.batch.duration')

    kafka_topic = config.get('Kafka configurations', 'kafka.topics')
    kafka_brokers = config.get('Kafka configurations', 'kafka.brokers')

    # CREATE SPARK CONTEXT
    # TODO: check whether is possible to set other variables such as: pyFiles, jars, ecc
    sc = SparkContext(
        appName = app_name,
        master  = spark_master
    )

    properties = ConfigParser.ConfigParser()
    properties.read(wd+'/PycharmProjects/TwitterSentimentAnalysis/db/db-properties.conf')
    db = properties.get('jdbc configurations', 'database')
    user = properties.get('jdbc configurations', 'user')
    passwd  = properties.get('jdbc configurations', 'password')

    sqlContext = SQLContext(sc)

    tableName = sc.broadcast(createUniqueTableName('NBM'))
    MYSQL_CONNECTION_URL = sc.broadcast(
        'jdbc:mysql://localhost:3306/' + db + '?user=' + user + '&password=' + passwd + '&useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC&useSSL=false')

    # CREATE TABLE SCHEMA
    schema = StructType([
        StructField("gt", IntegerType(), True),
        StructField("predicted", IntegerType(), True)])


    dfTableSchema = sqlContext.createDataFrame(sc.emptyRDD(), schema)
    dfTableSchema.write.jdbc(MYSQL_CONNECTION_URL.value, tableName.value, mode = 'error')

    # LOAD JDBC PROPERTIES
    df = sqlContext.read.format('jdbc')\
                        .options(url = MYSQL_CONNECTION_URL.value,
                                 dbtable = db+'.'+tableName.value
                                ).load()

    # CREATE STREAMING CONTEXT
    ssc = StreamingContext(
        sc,
        int(spark_batch_duration)
    )

    # setting checkpoint
    # ssc.checkpoint(".")

    #
    # LOADING AND COMPUTING TF's TRAINING MODEL
    print('Loading TRAINING_TF_MODEL...',end="")
    tf_training = sc.pickleFile('/Users/davidenardone/Desktop/TF_MODEL')
    print('done!')

    print('Computing TF-IDF MODEL...',end="")
    global idf_training
    idf_training = IDF().fit(tf_training)
    print('done!')

    print('Loading Naive Bayes Model...',end="")
    NBM = NaiveBayesModel.load(sc, "/Users/davidenardone/Desktop/NaiveBayesModel")
    print('done!')

    kafkaParams = {'metadata.broker.list"': kafka_brokers}

    # CREATE DIRECT KAFKA STREAM WITH BROKERS AND TOPICS
    streamData = KafkaUtils.createDirectStream(
        ssc,
        [kafka_topic],
        {"metadata.broker.list": kafka_brokers}
    )


    ######### FROM NOW ON, EACH ACTION OR TRANSFORMATION IS DONE ON A SINGLE INCOMING BATCH OF TWEETS #########

    # PRE-PROCESSING TWEETS DATA (TESTING)
    obj1 = TweetPreProcessing()
    tweet = streamData.map(lambda x: x[1]) \
                      .map(decodeUnicode)\
                      .flatMap(obj1.TweetBuilder)\


    #RETRIEVING TWEET's TEXT and LABEL
    # ZIPPING EACH TWEET WITH UNIQUE ID
    label = tweet.map(lambda tup: tup[0]) \
        .transform(lambda x: x.zipWithUniqueId()) \
        .map(lambda line: (line[1], int(line[0])))
    # int() casting string 'label' to int

    text = tweet.map(lambda tup: tup[1])

    #computing TF-IDF for each tweet and classifying it
    hashingTF = HashingTF()
    tfidf_testing = text.map(lambda tup: hashingTF.transform(tup)) \
                    .transform(lambda tup: idf_training.transform(tup)) \

    tweet_classified = tfidf_testing.map(lambda p: int(NBM.predict(p)))\
                                            .transform(lambda p: p.zipWithUniqueId()) \
                                            .map(lambda line: (line[1], line[0])) \
                                            # .pprint()

    # Here the ground truth and the predicted class are joined
    # so, for each tweet we have the following structure:
    # (class_predicted, ground truth) i.e. (4,0),(0,0)
    result = label.join(tweet_classified) \
            .map(lambda tup: tup[1]) \
            .foreachRDD(jdbcInsert)

    ssc.start()
    ssc.awaitTermination()



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