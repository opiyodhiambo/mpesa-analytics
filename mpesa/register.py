from mpesa_client import MpesaClient
from dotenv import load_dotenv
import os

load_dotenv()

consumer_key = os.getenv('CONSUMER_KEY')
consumer_secret = os.getenv('CONSUMER_SECRET')
shortcode = os.getenv('SHORTCODE')
response_type = os.getenv('RESPONSE_TYPE')
confirmation_url = os.getenv('CONFIRMATION_URL')
validation_url = os.getenv('VALIDATION_URL')

client = MpesaClient(
    consumer_key=consumer_key,
    consumer_secret=consumer_secret,
    shortcode=shortcode,
    confirmation_url=confirmation_url,
    validation_url=validation_url,
    access_token=None
)

if __name__ == "__main__":
    result = client.register_urls()
    print("Registration response:", result)