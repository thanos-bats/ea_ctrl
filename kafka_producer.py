from confluent_kafka import Producer
import json

class ProducerHandler:
    def __init__(self, bootstrap_servers):
        self.bootstrap_servers = bootstrap_servers
        print(f'Producer: bootstrap_servers {bootstrap_servers}')
        self.producer = Producer(bootstrap_servers)

    def delivery_report(self, err, msg):
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

    def send_message(self, topic, message_data):
        message_json = json.dumps(message_data)
        self.producer.produce(topic, value=message_json, callback=self.delivery_report)
        self.producer.flush()

    def close_producer(self):
        self.producer.flush()


if __name__ == '__main__':
    msg =  {
        "header": {
            "topicName": "10_CRAWLER_DATA", 
            "msgId": "CADmVnULgnsHyMlF6G2xY", 
            "sender": "CERTH", 
            "source": "Twitch", 
            "sentUtc": "2024-03-15T15:28:16Z"
        }, 
        "body": {
            "description": "twitch chat logs on user's spear_shot stream", 
            "data": [
                {
                    "id": "snpe97LgL3An7AqguNAsn", "caseId": "GgdlxmvLg12sNAsG1tbWH", "taskId": "K5JaaAqNTY4jd6iXTYhVp", "jobId": "cym2FnGvuo_4zYhWzcxFA"
                }
                # {
                #     "id": "LbJJkDti46jjFeprO_vVn", "caseId": "X_8CRMg73YFEJB98iEChq", "taskId": "66SrmoDUY6zBix85KhyWE", "jobId": "cym2FnGvuo_4zYhWzcxFA"
                # }
            ]
        }
    }
    bootstrap_servers = "localhost:9092"
    topic = "10_CRAWLER_DATA"

    print('Bootstrap Servers:', bootstrap_servers)
    print('Topic:', topic)

    config = {
        "bootstrap.servers": bootstrap_servers
    }
    # Instantiate the MessageProducer
    producer = ProducerHandler(config)
    producer.send_message(topic, msg)