from flask import request, jsonify

class MpesaAPI:
    def __init__(self):
        self.response_code = None
        self.response_text = None
    
    async def process_confirmation(self, confirmation_data):
        """
        This method processes the Mpesa confirmation response
        """
        data = request.get_json()
        print("M-Pesa Confirmation Data:", data)

    async def validate_transaction(self, validation_data):
        """
        This method is just a validation step
        """
        data = request.get_json()
        print("M-Pesa Validation Data:", data)
