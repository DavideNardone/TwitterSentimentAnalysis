from __future__ import print_function

import os
import time
import json
import csv
from kafka import KafkaProducer
import ConfigParser


if __name__ == "__main__":

    config = ConfigParser.ConfigParser()
    print (os.getcwd()+'/twitter-kakfa-producer/conf/producer.conf')
    config.read(os.getcwd()+'/twitter-kakfa-producer/conf/producer.conf')

    #reading configuration
    brookers = config.get('Kafka configurations', 'metadata.broker.list')
    kafka_topic = config.get('Kafka configurations', 'kafka.topic').replace('"', '''''')
    request_required_acks = config.get('Kafka configurations', 'request.required.acks')


    producer = KafkaProducer(
        bootstrap_servers = brookers,
        acks = int(request_required_acks)
    )

    #reading tweets from a file and send to the consumer

    # read text file tab-separated
    text_file = list(
        csv.reader(
            open('/Users/davidenardone/twitterDataset/twitter/test_data.txt', 'rU')
            # delimiter = '\t',
            # lineterminator='\r\n',
            # quoting=csv.QUOTE_ALL
        )
    )

    for row in text_file:
        time.sleep(1)
        # text = str(row)
        # print(text)
        jd = json.dumps(row).encode('ascii')
        producer.send(kafka_topic,jd)