from __future__ import print_function
from kafka import KafkaProducer
import os
import json
import time
import csv
import random
import ConfigParser


if __name__ == "__main__":

    config = ConfigParser.ConfigParser()
    print (os.getcwd()+'/twitter-kakfa-producer/conf/producer.conf')
    config.read(os.getcwd()+'/twitter-kakfa-producer/conf/producer.conf')

    #reading configuration
    brookers = config.get('Kafka configurations', 'metadata.broker.list')
    kafka_topic = config.get('Kafka configurations', 'kafka.topic')# .replace('"', '''''')
    request_required_acks = config.get('Kafka configurations', 'request.required.acks')

    # CREATING KAFKA PRODUCER
    producer = KafkaProducer(
        bootstrap_servers = brookers,
        acks = int(request_required_acks)
    )

    # READS TWEETS FROM A FILE AND SEND IT TO A CONSUMER

    # read text file tab-separated
    text_file = list(
        csv.reader(
            open('/twitterDataset/twitter/testing_data.txt', 'rU')
            # open('/Users/davidenardone/Desktop/kmeans_data.txt', 'rU')
            # delimiter = '\t',
            # lineterminator='\r\n',
            # quoting=csv.QUOTE_ALL
        )
    )

    # shuffling data
    random.shuffle(text_file)

    # SIMULATING TWEETS' STREAM
    for row in text_file:
        time.sleep(1)
        print(row)
        jd = json.dumps(row).encode('ascii')
        producer.send(kafka_topic,jd)
