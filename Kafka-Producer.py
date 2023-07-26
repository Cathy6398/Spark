from typing import List
from uuid import uuid4
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringSerializer,SerializationContext,MessageField
from confluent_kafka.schema_registry.json_schema import JSONSerializer
import pandas as pd


FILE_PATH="C:/Users/kirub/OneDrive/Documents/Spark/Case.csv"
columns=['case_id','province','city','group','infection_case','confirmed','latitude','longitude']

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


def get_case(file_path):
    df=pd.read_csv(file_path)
    df=df.iloc[:,0:]
    cases:List[Case]=[]
    for data in df.values:
        case=Case(dict(zip(columns,data)))
        cases.append(case)
        yield case

def case_to_dict(case:Case, ctx):
    """
    Returns a dict representation of a User instance for serialization.
    Args:
        user (User): User instance.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    Returns:
        dict: Dict populated with user attributes to be serialized.
    """

    # User._address must not be serialized; omit from dict
    return case.record


def report(err, msg):
    """
    Reports the success or failure of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        print("failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


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

    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    string_serializer = StringSerializer('utf_8')
    json_serializer = JSONSerializer(schema_str, schema_registry_client, case_to_dict)

    producer = Producer(sasl_conf())

    print("Producing user records to topic {}. ^C to exit.".format(topic))
    #while True:
        # Serve on_delivery callbacks from previous calls to produce()
    producer.poll(0.0)
    try:
        for case in get_case(file_path=FILE_PATH):

            print(case)
            producer.produce(topic=topic,
                            key=string_serializer(str(uuid4()), case_to_dict),
                            value=json_serializer(case, SerializationContext(topic, MessageField.VALUE)),
                            on_delivery=report)
    except KeyboardInterrupt:
        pass
    except ValueError:
        print("Invalid input, discarding record...")
        pass

    print("\nFlushing records...")
    producer.flush()

main("Covid_Case")