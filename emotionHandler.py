import requests
import os
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class emotionHandler:
    def __init__(self):
        # English emotion service configuration
        self.english_url = os.getenv('EN_EMOTION_URL')
        logger.info(f"English emotion service URL: {self.english_url}")

        # Greek emotion service configuration
        self.greek_url = os.getenv('GR_EMOTION_URL')
        logger.info(f"Greek emotion service URL: {self.greek_url}")

    def get_emotion(self, text, language):
        try:
            if language == 'en':
                url = self.english_url
            elif language == 'el':
                url = self.greek_url
            else:
                logger.warning(f"Unsupported language: {language}")
                return None

            logger.info(f"Requesting emotion analysis for {language} text")
            response = requests.post(url, json={'text': text})
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error in emotion analysis request: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error in emotion analysis: {str(e)}")
            return None

    