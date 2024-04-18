import requests
import os
from dotenv import load_dotenv

load_dotenv()

class emotionHandler:
    def __init__(self):
        self.base_url = os.getenv('BASE_URL')

    def get_emotion_outputs(self, input, lang):
        try:
            print('Before Emotion Analysis...')
            if lang == 'en':
                port = os.getenv('EN_PORT')
                endpoint = os.getenv('EN_NER')
                b_url = os.getenv('EN_URL')
            if lang == 'el':
                port = os.getenv('GR_PORT')
                endpoint = os.getenv('GR_NER')
                b_url = os.getenv('GR_URL')

            url = b_url + ":" + port + "/" + endpoint
            payload = {"text": input}
            response = requests.post(url, json=payload)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error making POST request: {e}")
            return None

    