import json
import os
from dotenv import load_dotenv
from kafka_consumer import ConsumerHandler
from kafka_producer import ProducerHandler
from emotionHandler import emotionHandler
from datastore_handler import DatastoreHandler
from confluent_kafka import KafkaError

# Load environment variables
load_dotenv()

def kafka_msg_structure(source):
    msg = {
        "header": {
            "topicName": os.getenv('EMOTION_TOPIC'),
            "source": source,
            "sentUTC": "2024-02-20T10:00:00.000Z",
        },
        "body": {}
    }
    return msg

def generate_done_message_data(documentId, taskId, jobId): 
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
        'auto.offset.reset': 'latest'
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
            msg_header = json_msg.get("header")
            msg_body = json_msg.get("body").get("data")
         
            inputs = []
            kafka_data = {"data": []}
            for msg in msg_body:
                resp = db.get_document(msg["documentId"])
                if resp is None:  # Check if the document exists
                    continue
                
                content = resp["content"]
                lang = resp["lang"]
                print(f"> Received msg {lang}\n")
                if resp["lang"] == "en" or resp["lang"] == 'el':  # Process only English or Greek content
                    inputs.append({"id": msg["documentId"], "content": content, "lang": resp["lang"]})
                else:
                    continue
                
                kafka_data["data"].append(generate_done_message_data(msg["documentId"], msg["taskId"], msg["jobId"]))

            if len(inputs) == 0:
                continue
                
            # Emotion analysis
            emotion_outputs = []
            for msg in inputs:
                output = emotion.get_emotion(msg.get("content"), msg.get("lang"))

                if (not output): 
                    continue
                emotion_outputs.append({"id": msg["id"], "entities": output})

            if len(emotion_outputs) == 0:
                continue
                
            for msg in emotion_outputs:
                print(f'msg:{msg}\n')
                documentId = msg.get("id")
                entities = msg.get("entities")
                print(f'entities: {entities}\n')
                
                # Skip if entities is None or contains an error
                if entities is None or "error" in entities:
                    print(f"  - Skipping document {documentId}: {entities.get('error', 'No entities found')}")
                    continue
                    
                entity_data = db.create_entity(
                    confidence=entities.get("confidence"),
                    prediction=entities.get("prediction"),
                    source=msg_header.get("source", "unknown").lower()
                )
                if not entity_data:
                    continue
                    
                _ = db.create_relationship(documentId, entity_data["id"], "hasEmotion")

            kafka_msg = kafka_msg_structure(msg_header["source"])
            kafka_msg["body"] = kafka_data
            producer.send_message(os.getenv('EMOTION_TOPIC'), kafka_msg)
    except KeyboardInterrupt:
        pass

if __name__ == '__main__':
    main()
