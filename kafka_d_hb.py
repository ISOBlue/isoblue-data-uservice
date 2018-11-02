#!/usr/bin/env python

import io
import sys
import re
import struct
import argparse
import time
import datetime
import json

import avro.schema
import avro.io

import requests

from struct import *
from kafka import KafkaConsumer

topic = 'debug'
server_addr = '128.46.71.204'

if __name__ == "__main__":
    # avro schema path
    schema_path = '../schema/d_hb.avsc'

    # load avro schema
    schema = avro.schema.parse(open(schema_path).read())

    consumer = KafkaConsumer(topic, group_id=None)

    for message in consumer:
        # disregard any message that does not have heartbeat key
        key_splited = message.key.split(':')
        if key_splited[0] != 'hb':
            continue

        # we have a heartbeat message, get the current (recv) timestamp
        recv_ts = time.time()

        # get the isoblue id
        isoblue_id = key_splited[1]

        # setup avro decoder
        bytes_reader = io.BytesIO(message.value)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(schema)
        hb_datum = reader.read(decoder)

        # read each field
        ts = hb_datum['timestamp']
        cellrssi = hb_datum['cellns']
        wifirssi = hb_datum['wifins']
        statled = hb_datum['statled']
        netled = hb_datum['netled']

        print ts, isoblue_id, cellrssi, wifirssi, statled, netled

        # get the day bucket from generation timestamp
        day_bucket = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d')
        hr_bucket = datetime.datetime.fromtimestamp(ts).strftime('%H')

        print 'day_bucket is:', day_bucket, 'hr_bucket is:', hr_bucket

        if len(hr_bucket) == 1
            hr_bucket = '0' + hr_bucket

        # construct the URL for PUT
        url = 'https://' + server_addr + \
            '/bookmarks/isoblue/device-index/' + \
            isoblue_id + '/day-index/' + day_bucket + '/' + \
            'hour-index/' + hr_bucket + ':00' + '/heartbeats'

        print url

        # construct the JSON object and headers
        hb_data = {
            ts: {
              'genTime': ts,
              'recTime': recv_ts,
              'interfaces': [
                {'type': 'cellular', 'rssi': cellrssi},
                {'type': 'wifi', 'rssi': wifirssi}
              ],
              'ledStatuses': [
                {'name': 'net', 'status': netled},
                {'name': 'stat', 'status': statled}
              ]
            }
        }

        headers = {
            'Authorization': 'Bearer def',
            'Content-Type': 'application/vnd.oada.harvest.1+json'
        }

        print json.dumps(hb_data, indent=2)

        # make the PUT request
        r = requests.put(url, data=json.dumps(hb_data), headers=headers, \
            verify=False)

        if r.status_code == 204:
            print 'PUT:', url, 'SUCCEEDED!'
        else:
            print 'PUT:', url, 'FAILED!'
            print 'status code:', r.status_code
            print 'error status:', r.text

        print '***************************************************************'
