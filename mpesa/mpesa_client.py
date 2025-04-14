import requests
from requests.auth import HTTPBasicAuth


class MpesaClient:
    def __init__(self, consumer_key, consumer_secret, shortcode, confirmation_url, validation_url=None, access_token=None):
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.shortcode = shortcode
        self.confirmation_url = confirmation_url
        self.validation_url = validation_url
        self.access_token = access_token
        self.base_url = "https://sandbox.safaricom.co.ke"

    def generate_access_token(self):

        authentication_url = f"{self.base_url}/oauth/v1/generate?grant_type=client_credentials"
        response = requests.get(authentication_url, auth=HTTPBasicAuth(self.consumer_key, self.consumer_secret))
        print("Status Code:", response.status_code)
        print("Response Text:", response.text)

        response.raise_for_status()
        self.access_token = response.json().get("access_token")

    def register_urls(self, response_type="Completed"):
        if not self.access_token:
            self.generate_access_token()

        url = f"{self.base_url}/mpesa/c2b/v1/registerurl"
        headers = {             
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

        body = {    
            "ShortCode": self.shortcode,
            "ResponseType": response_type,
            "ConfirmationURL":self.confirmation_url,
            "ValidationURL": self.validation_url
        }
        print("Sending registration payload:", body)
        response = requests.post(url, json=body, headers=headers)
        print("Mpesa Response ::", response)
        print("Response Text ::", response.text)
        response.raise_for_status()
        return response.json
