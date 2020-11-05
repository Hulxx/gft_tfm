# -*- coding: utf-8 -*-
"""
Created on Thu Nov  5 13:20:48 2020

@author: Usuario
"""

import argparse
import itertools
import logging
import datetime
import time
import base64
import json

#3rd Party Imports
import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
import six



def parse_json(line):
    '''Converts line from PubSub back to dictionary
    '''
    record = json.loads(line)
    return record


def decode_message(line):
    '''Decodes the encoded line from Google Pubsub
    '''
    return base64.urlsafe_b64decode(line)


def run(argv=None):
    '''Main method for executing the pipeline operation
    '''
    parser = argparse.ArgumentParser()

    parser.add_argument('--input_mode',
                        default='stream',
                        help='Streaming input or file based batch input')

    parser.add_argument('--input_topic',
                        default='projects/vanaurum/topics/stock-stream',
                        required=True,
                        help='Topic to pull data from.')

    parser.add_argument('--output_table', 
                        required=True,
                        help=
                        ('Output BigQuery table for results specified as: PROJECT:DATASET.TABLE '
                        'or DATASET.TABLE.'))

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    if known_args.input_mode == 'stream':
        pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:

        price = ( p
                | 'ReadInput' >> beam.io.ReadFromPubSub(topic=known_args.input_topic).with_output_types(six.binary_type))
                | 'Decode'  >> beam.Map(decode_message)
                | 'Parse'   >> beam.Map(parse_json) 
                | 'Write to Table' >> beam.io.WriteToBigQuery(
                        known_args.output_table,
                        schema=' timestamp:TIMESTAMP, stock_price:FLOAT',
                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))



if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
