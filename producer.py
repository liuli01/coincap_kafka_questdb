from os import times_result
import time, json
import numpy as np
import datetime as dt
import requests
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         json.dumps(x,default=str).encode('utf-8'))

print('Initialized Kafka producer at {}'.format(dt.datetime.utcnow()))


while True:
    
    uri = 'http://api.coincap.io/v2/assets/ethereum'
    res = requests.request("GET",uri)

    start_time = time.time()

    if (res.status_code==200):
    # read json response
        raw_data = json.loads(res.content)

        # add the schema
        data = {'schema': {
            'type': 'struct',
            'fields': [{'type': 'string', 'optional': False, 'field': 'currency'
                    }, {'type': 'float', 'optional': False, 'field': 'amount'
                    }, {'type': 'string', 'optional': False,
                    'field': 'timestamp'}],
            'optional': False,
            'name': 'Coincap',
            }, 'payload': {'timestamp': dt.datetime.utcnow(),
                        'currency': raw_data['data']['id'],
                        'amount': float(raw_data['data']['priceUsd'])}}

        print('API request succeeded at time {0}'.format(dt.datetime.utcnow()))
        
        producer.send(topic="topic_ETH",value=data)
        
        print('Sent record to topic at time {}'.format(dt.datetime.utcnow()))
         
    else:
        print('Failed API request at time {0}'.format(dt.datetime.utcnow()))

    end_time = time.time()
    time_inteval = end_time - start_time
    time.sleep(300 - time_inteval)