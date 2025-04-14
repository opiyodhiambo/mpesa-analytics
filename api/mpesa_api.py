from api.models import MpesaRequest

class MpesaAPI:
    async def process_confirmation(self, confirmation_data: MpesaRequest):
        print("M-Pesa Confirmation Data:", confirmation_data)
        return {"status": "confirmed"}

    async def validate_transaction(self, validation_data: MpesaRequest):
        print("M-Pesa Validation Data:", validation_data)
        return {"status": "validated"}