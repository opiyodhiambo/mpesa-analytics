import requests
from requests.auth import HTTPBasicAuth


class MpesaClient:
    def __init__(self, consumer_key, consumer_secret, shortcode, confirmation_url, validation_url=None):
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret,
        self.shortcode = shortcode
        self.confirmation_url = confirmation_url
        self.validation_url = validation_url
        self.access_token = None
        self.base_url = "https://sandbox.safaricom.co.ke"

    def authenticate(self):
        authentication_url = f"{self.base_url}/oauth/v1/generate?grant_type=client_credentials"
        response = requests.get(authentication_url, auth=HTTPBasicAuth(consumer_key, consumer_secret))
        response.raise_for_status()
        self.access_token = response.json().get("access_token")

    def register_urls(self, response_type="Completed"):
        if not self.access_token:
            self.authenticate()

        url = f"{self.base_url}/mpesa/c2b/v1/registerurl"
        headers = {             
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        body = {    
            "ShortCode": shortcode,
            "ResponseType": response_type,
            "ConfirmationURL":confirmation_url
        }

        response = requests.post(url, json=body, headers=headers)
        response.raise_for_status()
        return response.json
