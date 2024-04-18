from confluent_kafka import Consumer, KafkaError
import os
import json
from dotenv import load_dotenv

load_dotenv()

class ConsumerHandler:
    def __init__(self, topics, config):
        print(f"Consumer Configuration: {config}")
        print(f"Subscribe into topics {topics}")
        self.consumer = Consumer(config)
        self.consumer.subscribe(topics)

    def poll(self, timeout=0):
        """ Read one message (or callback) from the bus if available.

        Reads only from the subscribed topics.

        Return the message/callback/None(if there is not a message or callback on bus).
        """
        return self.consumer.poll(timeout=timeout)
    
if __name__ == '__main__':
    topic = os.getenv('NER_TOPIC')
    cons_config = {
        'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
        'group.id': os.getenv('GROUP_ID'),
        'auto.offset.reset': 'latest'
    }
    consumer = ConsumerHandler(topic, cons_config)
    print(f'> Starting listering for topic {topic} with configs {cons_config}')
    while True:
            msgs = consumer.poll(0.0)

            if msgs is None:
                continue
            if msgs.error():
                if msgs.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print('Error: {}'.format(msgs.error()))
                    break

            msg_values = json.loads(msgs.value().decode('utf-8'))
            print(f"> Message received: {msg_values}")