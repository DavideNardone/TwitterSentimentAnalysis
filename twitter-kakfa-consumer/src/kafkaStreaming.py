#changes
#changes
#changes
#changes

from __future__ import print_function

import sys
import json


from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonStreamingKafkaWordCount")
    ssc = StreamingContext(sc, 1)

    zkQuorum, topic = sys.argv[1:]
    # kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": zkQuorum})
    # kvs = KafkaUtils.createStream(ssc, [zkQuorum], {"metadata.broker.list": "localhost:2181"})

    lines = kvs.map(lambda x: x[1]).pprint()
    # counts = lines.flatMap(lambda line: line.split(" ")) \
    #     .map(lambda word: (word, 1)) \
    #     .reduceByKey(lambda a, b: a+b)
    # counts.pprint()
    ssc.start()
ssc.awaitTermination()