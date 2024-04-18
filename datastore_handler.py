import requests
import os
import uuid
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()
label_mapping = {
    
}

class DatastoreHandler:
    def __init__(self):
        self.base_url = os.getenv('NEO4J_URL')

    def get_document(self, document_id):
        url = f'{self.base_url}/documents?id={document_id}'
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error making GET request: {e}")
            return None
    
    def create_entity(self, confidence, prediction):
        current_utc_time = datetime.utcnow().replace(tzinfo=timezone.utc)
        formatted_utc_time = current_utc_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        input = {
            "domainId":f"emotion:{prediction.lower()}:{str(uuid.uuid4())}",
            "title": None,
            "name":prediction,
            "source":"web",
            "type":f"emotion:{prediction.lower()}",
            "discoveredAt": formatted_utc_time,
            "attributes":{
                "confidence": confidence
            }
        }
        try:
            url = f'{self.base_url}/entities'
            response = requests.post(url, json=input)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error making POST request: {e}")
            return None

    
    def create_relationship(self, source_id, target_id, relationship_name):
        input = {
            "sourceNodeId": source_id,
            "targetNodeId": target_id,
            "type": relationship_name
        }
        try:
            url = f'{self.base_url}/relationships'
            response = requests.post(url, json=input)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error making POST request: {e}")
            return None

    def patch_document(self, documentId, nerContent):
        payload = {
            "id": documentId,
            "attributes": {
                "nerContent": nerContent
            }
        }
        try:
            url = f'{self.base_url}/documents'
            response = requests.patch(url, json=payload)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error making PATCH request: {e}")
            return None
        
    def get_entity(self, document_id, relationshipName):
        url = f'{self.base_url}/entities?documentId={document_id}&relationshipType={relationshipName}'
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error making GET request: {e}")
            return None
    