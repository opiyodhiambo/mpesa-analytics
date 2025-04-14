from mpesa_client import MpesaClient

consumer_key = ""
consumer_secret = ""
shortcode = ""
response_type = "Completed"
confirmation_url = ""

client = MpesaClient(
    consumer_key=consumer_key,
    consumer_secret=consumer_secret,
    shortcode=shortcode,
    confirmation_url=confirmation_url
)

if __name__ == "__main__":
    result = client.register_urls()
    print("Registration response:", result)