# -*- coding: utf-8 -*-
"""
Created on Thu Nov  5 13:21:44 2020

@author: Usuario
"""

import time
import datetime
import json
import base64

# Local imports
from stock_generator.stock_generator import StockGenerator

# 3rd Party Imports
from google.cloud import pubsub

PROJECT = 'vanaurum'
TOPIC = 'stock-stream'


def pub_callback(message_future):
    # When timeout is unspecified, the exception method waits indefinitely.
    topic = 'projects/{}/topics/{}'.format(PROJECT, TOPIC)
    if message_future.exception(timeout=30):
        print('Publishing message on {} threw an Exception {}.'.format(
            topic, message_future.exception()))
    else:
        print(message_future.result())


def main():
    # Publishes the message 'Hello World'
    publisher = pubsub.PublisherClient()
    topic = 'projects/{}/topics/{}'.format(PROJECT, TOPIC)
    stock_price = StockGenerator(mu = 1.001, sigma = 0.001, starting_price = 100)

    while True:

        # Pause for 1 second to mimic second-by-second data
        time.sleep(1)
        price = next(stock_price)
        timestamp = str(datetime.datetime.utcnow()) # str to make json serializable

        # Create the body of the message we want to publish in the stream
        body = {
            'stock_price': price,
            'timestamp': timestamp,
        }

        # Dump to json and encode as string, as is required by Pubsub
        str_body = json.dumps(body)
        data = base64.urlsafe_b64encode(bytearray(str_body, 'utf8'))
        message_future = publisher.publish(
            topic, 
            data=data,
            )
        message_future.add_done_callback(pub_callback)

if __name__ == '__main__':
    main()