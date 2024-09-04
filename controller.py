import json
import os
from dotenv import load_dotenv
from kafka_consumer import ConsumerHandler
from confluent_kafka import KafkaError

from kafka_producer import ProducerHandler
from datastore_handler import DatastoreHandler
from emotionHandler import emotionHandler
from datetime import datetime, timezone

load_dotenv()

def kafka_msg_structure(source):
    current_utc_time = datetime.utcnow().replace(tzinfo=timezone.utc)
    formatted_utc_time = current_utc_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    msg = {
        "header": {
            "topicName": os.getenv('EMOTION_TOPIC'),
            "source": source,
            "sentUTC": formatted_utc_time,
        },
        "body": {}
    }
    return msg

def generate_sentiment_done_message_data(documentId, taskId, jobId): 
    msg_data = {
        "taskId": taskId,
        "jobId": jobId,
        "documentId": documentId
    }
    return msg_data

def main():
    cons_config = {
        'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
        'group.id': os.getenv('GROUP_ID'),
        'auto.offset.reset': 'earliest'
    }
    consumer = ConsumerHandler([os.getenv('CRAWLER_TOPIC')], cons_config)
    db = DatastoreHandler()
    emotion = emotionHandler()
    producer = ProducerHandler({"bootstrap.servers": os.getenv('KAFKA_BOOTSTRAP_SERVERS')})

    try:
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

            # Parse the JSON message
            json_msg = json.loads(msgs.value().decode('utf-8'))
            msg_header =  json_msg.get("header")
            msg_body = json_msg.get("body").get("data")
         
            inputs = []
            kafka_data = {"data": []}
            for msg in msg_body:
                resp = db.get_document( msg["documentId"])
                if resp is None: # Check if the document exists
                    continue
                
                content = resp["content"]
                lang = resp["lang"]
                print(f"> Received msg: {content} | {lang}")
                if resp["lang"] == "en" or resp["lang"] == 'el': # If the lang is on EN I will call the NER_ENGLISH or If the lang in on GR I will call the NER_GREEK
                    inputs.append({"id": msg["documentId"], "content": content, "lang": resp["lang"]})
                else:
                    continue

                kafka_data["data"].append(generate_sentiment_done_message_data(msg["documentId"], msg["taskId"], msg["jobId"]))

            if len(inputs) == 0:
                continue
            
            # Sentiment tool
            emotion_outputs = []
            for msg in inputs:
                output = emotion.get_emotion_outputs(msg.get("content"), msg.get("lang"))

                if (not output): 
                    continue
                emotion_outputs.append({"id": msg["id"], "entities": output})

            if len(emotion_outputs) == 0:
                continue
            
            for msg in emotion_outputs:
                print(f'msg:{msg}')
                documentId = msg.get("id")
                entities = msg.get("entities")
                print(f'entities: {entities}')
                if entities is None:
                    continue
                entity_data = db.create_entity(entities.get("confidence"), entities.get("prediction"))
                if not entity_data:
                    continue
                    
                relationship = db.create_relationship(documentId, entity_data["id"], "hasEmotion")

            kafka_msg = kafka_msg_structure(msg_header["source"])
            kafka_msg["body"] = kafka_data
            producer.send_message(os.getenv('EMOTION_TOPIC'), kafka_msg)
    except KeyboardInterrupt:
        pass

if __name__ == '__main__':
    main()
