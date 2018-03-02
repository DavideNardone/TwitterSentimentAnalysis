from __future__ import print_function
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from TweetPreProcessing import TweetPreProcessing
from pyspark.mllib.classification import NaiveBayesModel
from pyspark.sql import Row
import ConfigParser
import datetime
import os
import json
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF
from pyspark.sql.types import *    
    
if __name__ == "__main__":
    
    # TRAINING MODEL
    
     conf = (SparkConf().setMaster("local[4]")
         .set("spark.app.name","TrainingBayesModel")
         .set("spark.executor.cores", "4")
         .set("spark.cores.max", "4")
         .set('spark.executor.memory', '6g')
     )
    
     sc = SparkContext(conf=conf)
     rdd = sc.parallelize(input_data, numSlices=4)
     ssc = StreamingContext(sc, 1)
    
     print('Loading dataset...')
    
     allData = sc.textFile(
                         "/twitterDataset/twitter/training_data.txt",
                           use_unicode=False
                          )
    
     obj1 = TweetPreProcessing()
    
     training = allData.map(lambda x: x.replace("\'",''))\
                   .map(lambda x: x.split('",'))\
                   .flatMap(obj1.TweetBuilder)
    
    training, test = data.randomSplit([0.7, 0.3], seed=0)
    
     tf_val = 1024
    
     hashingTF = HashingTF(tf_val)
    
     print('Computing TF model...')
    
     tf_training = training.map(lambda tup: hashingTF.transform(tup[1]))
    
     print('Saving TF_MODEL...')
    
     tf_training.saveAsPickleFile("/model/TF_MODEL_"+str(tf_val))
    
     idf_training = IDF().fit(tf_training)
    
     print('Computing TF-IDF...')
    
     tfidf_training = idf_training.transform(tf_training)
    
     tfidf_idx = tfidf_training.zipWithIndex()
    
     training_idx = training.zipWithIndex()
    
     idx_training = training_idx.map(lambda line: (line[1], line[0]))
    
     idx_tfidf = tfidf_idx.map(lambda l: (l[1], l[0]))
    
     joined_tfidf_training = idx_training.join(idx_tfidf)
    
     training_labeled = joined_tfidf_training.map(lambda tup: tup[1])
    
     labeled_training_data = training_labeled.map(lambda k: LabeledPoint(k[0][0], k[1]))
    
     print('Computing Naive Bayes Model...')
    
     model = NaiveBayes.train(labeled_training_data, 1.0)
    
     print('Saving Naive Bayes Model...')
    
     model.save(sc, "/model/NBM/NaiveBayesModel_"+str(tf_val))
