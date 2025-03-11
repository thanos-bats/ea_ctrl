import requests
import os
from dotenv import load_dotenv

load_dotenv()

class emotionHandler:
    def __init__(self):
        self.greek_url = os.getenv('GR_EMOTION_URL') + ":" + os.getenv('GR_EMOTION_PORT') + "/" + os.getenv('GR_EMOTION_ENDPOINT')  # Adjust port as needed
        self.english_url = os.getenv('EN_EMOTION_URL') + ":" + os.getenv('EN_EMOTION_PORT') + "/" + os.getenv('EN_EMOTION_ENDPOINT')  # Adjust port as needed

    def get_emotion_outputs(self, text, lang):
        try:
            # Prepare the input data
            input_data = {
                "text": text
            }

            # Select endpoint based on language
            emotion_url = self.greek_url if lang == 'el' else self.english_url

            # Make POST request to the appropriate endpoint
            response = requests.post(emotion_url, json=input_data)
            
            if response.status_code == 200:
                result = response.json()
                return result  # Returns {'prediction': 'emotion', 'confidence': probability}
            else:
                print(f"Error: Received status code {response.status_code}")
                return None

        except Exception as e:
            print(f"Error in emotion analysis: {str(e)}")
            return None

    