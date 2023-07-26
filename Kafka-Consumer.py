import pandas as pd
from confluent_kafka import Producer
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from pymongo import MongoClient
from bson  import json_util
from json import loads
import json

API_KEY = 'PMAEZB6CITQPZDA6'
ENDPOINT_SCHEMA_URL  = 'https://psrc-3508o.westus2.azure.confluent.cloud'
API_SECRET_KEY = 'BtK1vZaxc8dlF+y0LscILocnfCuRSFF+/D49CQCJphSsj6z01CzYaREalNQPywu0'
BOOTSTRAP_SERVER = 'pkc-4rn2p.canadacentral.azure.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 'ELWHBS2WYH75CFRM'
SCHEMA_REGISTRY_API_SECRET = 'tR6rOLIz9skjSSH7EbL3zmet4UlXF6FcvZPtnyM2sgkD+NgK9QREsO4QR20pbDgV'


def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf

def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }

class Case:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record
   
    @staticmethod
    def dict_to_case(data:dict,ctx):
        return Case(record=data)

    def __str__(self):
        return f"{self.record}"

def main(topic):

   schema_str = """
   {
  "$id": "http://example.com/myURI.schema.json",
  "$schema": "http://json-schema.org/draft-07/schema#",
  "additionalProperties": false,
  "description": "Sample schema to help you get started.",
  "properties": {
    "case_id": {
      "description": "The type(v) type is used.",
      "type": "number"
    },
    "city": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "confirmed": {
      "description": "The type(v) type is used.",
      "type": "number"
    },
    "group": {
      "description": "The type(v) type is used.",
      "type": "boolean"
    },
    "infection_case": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "latitude": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "longitude": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "province": {
      "description": "The type(v) type is used.",
      "type": "string"
    }
  },
  "title": "SampleRecord",
  "type": "object"
}
    """
   json_deserializer = JSONDeserializer(schema_str,from_dict=Case.dict_to_case)
   consumer_conf = sasl_conf()
   consumer_conf.update({
                     'group.id': 'group1',
                     'auto.offset.reset': "earliest"})
   consumer = Consumer(consumer_conf)
   consumer.subscribe([topic])


   while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            case = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if case is not None:
                print("User record {}: case: {}\n".format(msg.key(), case))
        except KeyboardInterrupt:
            break
        
   consumer.close()
        
main("Covid_Case")