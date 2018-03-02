from __future__ import print_function
from TweetPreProcessing import TweetPreProcessing
from pyspark.mllib.clustering import StreamingKMeans
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
import ConfigParser
import datetime
import json
import os
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF
from pyspark.mllib.feature import PCA
from pyspark.sql.types import *


def decodeUnicode(text):

    text = json.loads(text.decode('ascii'))

    text_list = []
    # turn unicode list elements into string element
    for word in text:
        text_list.append(word.encode("utf-8"))

    return text_list

# Lazily instantiated global instance of SQLContext
def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

def jdbcInsert(rdd):

    #  Get the singleton instance of SQLContext
    sqlContext = getSqlContextInstance(rdd.context)

    # Convert RDD[String] to RDD[Row] to DataFrame
    if pca_mode.value==1:
        rowRdd = rdd.map(lambda w: Row(x = float(w[0][0]), y = float(w[0][1]), gt = w[1][0], predicted = w[1][1]))
    else:
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
    # table_name = name+'_'+day+'_'+hour+'_'+min+'_'+sec
    table_name = 'test_kmeans'

    return table_name



if __name__ == "__main__":

    config = ConfigParser.ConfigParser()
    config.read(os.getcwd()+'/TwitterSentimentAnalysis/twitter-kakfa-consumer/conf/consumer.conf')

    # READING CONFIGURATION
    app_name = config.get('Spark configurations', 'spark.app.name')
    spark_master = config.get('Spark configurations', 'spark.master')
    spark_batch_duration = config.get('Spark configurations', 'spark.batch.duration')

    kafka_topic = config.get('Kafka configurations', 'kafka.topic')
    kafka_brokers = config.get('Kafka configurations', 'kafka.broker')

    # CREATE SPARK CONTEXT
    # TODO: check whether is possible to set other variables such as: pyFiles, jars, ecc
    sc = SparkContext(
        appName = app_name,
        master  = spark_master
    )

    properties = ConfigParser.ConfigParser()
    properties.read(os.getcwd()+'/TwitterSentimentAnalysis/db/db-properties.conf')
    db = properties.get('jdbc configurations', 'database')
    user = properties.get('jdbc configurations', 'user')
    passwd  = properties.get('jdbc configurations', 'password')

    sqlContext = SQLContext(sc)

    tableName = sc.broadcast(createUniqueTableName('k_means'))
    MYSQL_CONNECTION_URL = sc.broadcast(
        'jdbc:mysql://localhost:3306/' + db
         + '?user=' + user
         + '&password=' + passwd
         + '&useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC&useSSL=false')

    # CREATE TABLE SCHEMA
    schema = StructType([
        StructField("x", FloatType(), True),
        StructField("y", FloatType(), True),
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

    obj2 = TweetPreProcessing()

    # LOADING TRAINING DATA FROM FILE
    data = sc.textFile(
        os.getcwd()+"/twitterDataset/twitter/training_data.txt",
        use_unicode = False
    )

    # PRE-PROCESSING TWEETS DATA (TRAINING)
    # i.e '"0","1468031800","Mon Apr 06 23:21:13 PDT 2009","NO_QUERY","Leggetron","It's 6.20pm - early days in a looooooong night at work "'
    trainingData = data.map(lambda x: x.replace("\'", '')) \
        .map(lambda x: x.split('",')) \
        .map(lambda x: x.replace('"',''))\
        .flatMap(obj2.TweetBuilder)

    # whether set to '1', 'pca_mode' allows to use data projection on principal components
    pca_mode = sc.broadcast(0)
    low_dim = 2
    feature_dim = 4096 # 1048576
    k = feature_dim

    # LOADING AND COMPUTING TF's TRAINING MODEL
    print('Loading TRAINING_TF_MODEL...')
    tf_training = sc.pickleFile(os.getcwd()+'/model/TF/TF_MODEL_'+str(feature_dim))
    print('done!')

    print('Computing TF-IDF MODEL...')
    idf_training = IDF(minDocFreq=5).fit(tf_training)
    tfidf_training = idf_training.transform(tf_training)
    print('done!')

    # APPLYING PCA ON TRAINING DATA
    if pca_mode.value==1:
        print('Applying PCA on training data...')
        PCA_model = PCA(low_dim).fit(tfidf_training)
        tfidf_training = PCA_model.transform(tfidf_training)
        k = low_dim

    # pcArray = model.transform(tfidf_training.first()).toArray()

    #setting checkpoint
    # ssc.checkpoint("/Users/davidenardone/Desktop/checkpoint")

    # CREATING DStream FROM TRAINING'S RDD
    trainingQueue = [tfidf_training]
    trainingStream = ssc.queueStream(trainingQueue)

    # CREATING A K-MEANS MODEL WITH RANDOM CLUSTERS SPECIFYING THE NUMBER OF CLUSTERS TO FIND
    model = StreamingKMeans(k=2, decayFactor=1.0,timeUnit='batches').setRandomCenters(k, 1.0, 0)

    # print("K centers: " + str(model.latestModel().centers))

    # TRAINING THE MODEL ON THE TRAINING TWEET'S DATA
    print('Training K-means Model...')
    model.trainOn(trainingStream)
    print('done!')


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
                      .map(decodeUnicode) \
                      .flatMap(obj1.TweetBuilder)

    #RETRIEVING TWEET's TEXT and LABEL
    #ZIPPING EACH TWEET WITH UNIQUE ID
    label = tweet.map(lambda tup: tup[0])\
                 .transform(lambda x: x.zipWithUniqueId())\
                 .map(lambda line: (line[1], line[0]))\

    text = tweet.map(lambda tup: tup[1])


    # COMPUTING TF FOR EACH INCOMING TWEET
    # AND FITTING IDF'S TRAINING MODEL ON THE INCOMING BATCH
    hashingTF = HashingTF(feature_dim)

    tfidf_testing = text.map(lambda tup: hashingTF.transform(tup)) \
                    .transform(lambda tup: idf_training.transform(tup)) \

    # FITTING/APPLYING PCA' TRAINING MODEL ON THE INCOMING BATCH
    if pca_mode.value==1:
        tfidf_testing = tfidf_testing.transform(lambda tup: PCA_model.transform(tup)) \

    # ZIPPING EACH INCOMING TWEET WITH UNIQUE ID
    feature_testing_zipped = tfidf_testing.transform(lambda x: x.zipWithUniqueId()) \
                    .map(lambda line: (line[1], line[0])) \
                    # .pprint()

    # JOINING FEATURE AND LABEL FOR EACH INCOMING TWEET
    # (0, (DenseVector([0.9679, 0.6229]), '4'))
    feature_and_label = feature_testing_zipped.join(label) \

    # CREATING LABELING DATA
    labeled_data = feature_and_label.map(lambda k: LabeledPoint(k[1][1], k[1][0])) \

    # PREDICTING THE CLUSTER WHERE THE TWEET BELONG TO
    result = model.predictOnValues(labeled_data.map(lambda lp: (lp.label, lp.features)))

    # ZIPPING EACH RESULT
    result_zipped = result.transform(lambda x: x.zipWithUniqueId()) \
                        .map(lambda line: (line[1], line[0]))\

    # PREPARING OUTPUT DATA
    # structure ( (gt, cluster predicted),(features))
    # i.e. (DenseVector([0.9679, 0.6229]), (4.0, 0))
    rr = feature_and_label.join(result_zipped)\
                            .map(lambda x: (x[1][0][0], x[1][1]) if (pca_mode.value == 1) else (x[1][1])) \
                            .foreachRDD(jdbcInsert)\


    ssc.start()
    ssc.awaitTermination()
    # ssc.stop()
