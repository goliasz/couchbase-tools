#!/usr/bin/env python

import json
import uuid
from kafka import KafkaConsumer
from couchbase.bucket import Bucket
import argparse

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description="Kafka 2 Couchbase")
  parser.add_argument('--couchbase', default="couchbase://localhost/privela2")
  parser.add_argument('--couchbase_ttl', default="0")
  parser.add_argument('--kafka_bootstrap_srvs', default="localhost:9092")
  parser.add_argument('--kafka_group_id', default="privela2")
  parser.add_argument('--kafka_source_topic', default="snowplow-enriched-good-json")


  args = parser.parse_args()
  print "Couchbase",args.couchbase
  print "Couchbase TTL",args.couchbase_ttl
  print "Kafka boostrap servers",args.kafka_bootstrap_srvs
  print "Kafka group id",args.kafka_group_id
  print "Kafka source topic",args.kafka_source_topic

  c_couch_conn_str = args.couchbase
  ttlv = int(args.couchbase_ttl) 
  #
  consumer = KafkaConsumer(bootstrap_servers=args.kafka_bootstrap_srvs, group_id=args.kafka_group_id)
  consumer.subscribe([args.kafka_source_topic])

  snowplowBucket = Bucket(c_couch_conn_str)

  for msg in consumer:
    key = str(uuid.uuid4())
    msgj = json.loads(msg.value)
    #print msgj
    print "Write to couchbase. Key=",key
    snowplowBucket.upsert(key=key,value=msgj,cas=0,ttl=ttlv)
    #snowplowBucket.upsert(key=key,value=msgj)
