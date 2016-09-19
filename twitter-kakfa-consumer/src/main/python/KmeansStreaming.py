from __future__ import print_function


from TweetPreProcessing import TweetPreProcessing

# $example on$
from pyspark.mllib.clustering import StreamingKMeans
import ConfigParser
import json
import re
import numpy as np
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from lightning import Lightning
from pyspark.mllib.linalg import Vectors,DenseVector,SparseVector
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF
from pyspark.mllib.feature import PCA
# $example off$

# $example on$
# we make an input stream of vectors for training,
# as well as a stream of vectors for testing


def parse(lp):
    label = float(lp[lp.find('(') + 1: lp.find(')')])
    vec = Vectors.dense(lp[lp.find('[') + 1: lp.find(']')].split(','))

    return LabeledPoint(label, vec)



def fun1(lp):
    lp = lp[1]
    lp = json.loads(lp.decode('ascii'))

    # turn unicode list elements into list of string element
    for word in lp:
        loc = lp.index(word)
        lp[loc] = str(word).strip()


    ########### FOR TRAINING SET (STREAMING) ###########

    # Convert list to string
    lp = ' '.join(lp)

    label = float(lp[lp.find('(') + 1: lp.find(')')])
    vec =lp[lp.find('[') + 1: lp.find(']')].split(',')

    # Convert list to string
    vec = ' '.join(vec)

    # Convert string to list
    vec = DenseVector(vec.split(" "))

    # print(vec)

    # convert list to float
    # lp = [float(i) for i in lp]

    # print(type(lp))
    # print(lp)

    ########### FOR TRAINING SET (STREAMING) ###########

    return LabeledPoint(label, vec)



def test2(lp):

    # print(type(lp))
    return lp.zipWithIndex()


def fun2(lp):

    arr = lp.toArray()
    arr = np.array(arr).tolist()
    # print(arr)
    # print(type(arr))
    # print(DenseVector(lp))
    # exit(-1)
    # print(type(lp))

    return arr


def test(lp):

    lp = lp.toArray()
    print (lp)

    model = PCA(2).fit(sc.parallelize(data))

    # print(type(lp))

    return lp



if __name__ == "__main__":
    # sc = SparkContext(appName="StreamingKMeansExample")  # SparkContext
    # ssc = StreamingContext(sc, 1)

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

    obj2 = TweetPreProcessing()

    #loading training data
    data = sc.textFile(
        "/Users/davidenardone/twitterDataset/twitter/training_data.txt",
        use_unicode = False
    )
    # .map(lambda line: Vectors.dense([float(x) for x in line.strip().split(' ')]))
    trainingData = data.map(lambda x: x.replace("\'", '')) \
        .map(lambda x: x.split('",')) \
        .flatMap(obj2.ProcessTrainingTweets)

    # print(trainingData.collect())

    feature_dim = 512

    low_dim = 2

    print('Loading TRAINING_TF_MODEL...')
    tf_training = sc.pickleFile('/Users/davidenardone/Desktop/TF_MODEL_512')
    print('Computing TF-IDF MODEL...')
    idf_training = IDF().fit(tf_training)
    tfidf_training = idf_training.transform(tf_training)
                                 # .map(fun2)

    #applying PCA on training data
    print('Applying PCA on training data...')
    PCA_model = PCA(low_dim).fit(tfidf_training)

    pca_tfidf_training = PCA_model.transform(tfidf_training)

    # pcArray = model.transform(tfidf_training.first()).toArray()

    #setting checkpoint
    # ssc.checkpoint("checkpoint")

    # computing tf-idf for training
    # hashingTF = HashingTF(feature_dim)
    # print('computing TF-IDF...')
    # tf_training = trainingData.map(lambda tup: hashingTF.transform(tup[1]))
    # idf_training = IDF().fit(tf_training)
    # tfidf_training = idf_training.transform(tf_training)


    #creating DStream from RDD
    trainingQueue = [pca_tfidf_training]
    trainingStream = ssc.queueStream(trainingQueue)


    kafkaParams = {'metadata.broker.list"': kafka_brokers}
    # #
    # Create direct kafka stream with brokers and topics
    data = KafkaUtils.createDirectStream(
        ssc,
        [kafka_topic],
        {"metadata.broker.list": kafka_brokers}
    )

    testingStream = data.map(fun1)\
    #             # .pprint()

    obj1 = TweetPreProcessing()
    lines = data.map(lambda x: x[1])
    # #
    tweet = lines.flatMap(obj1.TweetBuilder)

    text =   tweet.map(lambda tup: tup[1])
    # #              # .pprint()
    label = tweet.map(lambda tup: tup[0])\
                 .transform(lambda x: x.zipWithUniqueId())\
                 .map(lambda line: (line[1], line[0]))
                 # .pprint()

    hashingTF = HashingTF(feature_dim)

    #computing TF-IDF for each tweet to classify
    tfidf_testing = text.map(lambda tup: hashingTF.transform(tup)) \
                    .transform(lambda tup: idf_training.transform(tup)) \

    #applying PCA on testing data
    pca_tfidf_testing = tfidf_testing.transform(lambda tup: PCA_model.transform(tup))\
                    # .pprint()

                    # .map(fun2)

    testing_zipped = pca_tfidf_testing.transform(lambda x: x.zipWithUniqueId()) \
                    .map(lambda line: (line[1], line[0])) \
                    # .pprint()

    testing = testing_zipped.join(label)\
                      # .pprint()

    testing_labeled = testing.map(lambda tup: tup[1])\
                             # .pprint()

    # # We create a model with random clusters and specify the number of clusters to find
    model = StreamingKMeans(k=3, decayFactor=0.3).setRandomCenters(low_dim, 1.0, 0)
    #
    # # Now register the streams for training and testing and start the job,
    # # printing the predicted cluster assignments on new data points as they arrive.
    print('Training K-means Model...')
    model.trainOn(trainingStream)

    # result = model.predictOn(training) #work
    # result.pprint()

    labeled_data = testing_labeled.map(lambda k: LabeledPoint(k[1], k[0]))\
                                  # .pprint()
    result = model.predictOnValues(labeled_data.map(lambda lp: (lp.label, lp.features)))
    result.pprint()


    # print("Final centers: " + str(model.latestModel().centers))
    #
    ssc.start()
    ssc.awaitTermination()
    # ssc.stop()



