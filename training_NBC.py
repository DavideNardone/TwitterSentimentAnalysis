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
                         "/Users/davidenardone/twitterDataset/twitter/training_data.txt",
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
    
     tf_training.saveAsPickleFile("/Users/davidenardone/Desktop/MODEL/TF_MODEL_"+str(tf_val))
    
     idf_training = IDF().fit(tf_training)
    
     print('computing TF-IDF...')
    
     tfidf_training = idf_training.transform(tf_training)
    
     tfidf_idx = tfidf_training.zipWithIndex()
    
     training_idx = training.zipWithIndex()
    
     idx_training = training_idx.map(lambda line: (line[1], line[0]))
    
     idx_tfidf = tfidf_idx.map(lambda l: (l[1], l[0]))
    
     joined_tfidf_training = idx_training.join(idx_tfidf)
    
     training_labeled = joined_tfidf_training.map(lambda tup: tup[1])
    
     labeled_training_data = training_labeled.map(lambda k: LabeledPoint(k[0][0], k[1]))
    
     print('computing Naive Bayes Model...')
    
     model = NaiveBayes.train(labeled_training_data, 1.0)
    
     print('Saving Naive Bayes Model...')
    
     model.save(sc, "/Users/davidenardone/Desktop/MODEL/NBM/NaiveBayesModel_"+str(tf_val))
