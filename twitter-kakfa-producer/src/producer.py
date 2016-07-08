from __future__ import print_function


import os
# import sys
import time
from kafka import KafkaProducer
import ConfigParser

if __name__ == "__main__":

    config = ConfigParser.ConfigParser()
    config.read('/Users/davidenardone/PycharmProjects/TwitterSentimentAnalysis/twitter-kakfa-producer/conf/test.conf')

    #reading configuration
    brookers = config.get('Kafka configurations', 'metadata.broker.list')
    kafka_topic = config.get('Kafka configurations', 'kafka.topic').replace('"', '''''')
    request_required_acks = config.get('Kafka configurations', 'request.required.acks')

    print(os.getcwd()+'/twitterDataset/twitter/testdata.txt')
    producer = KafkaProducer(
                            bootstrap_servers = brookers,
                            acks = int(request_required_acks)
                            )

    #reading tweets from a file and send to the consumer
    with open('../../twitterDataset/twitter/testdata.txt') as f:
            for line in f:
                time.sleep(1)
                producer.send(kafka_topic, line)