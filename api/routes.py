from fastapi import APIRouter, HTTPException
from api.models import MpesaRequest

router = APIRouter()

class MpesaAPI:
    async def process_confirmation(self, confirmation_data: MpesaRequest):
        print("M-Pesa Confirmation Data:", confirmation_data)
        return {"status": "confirmed"}

    async def validate_transaction(self, validation_data: MpesaRequest):
        print("M-Pesa Validation Data:", validation_data)
        return {"status": "validated"}

mpesa_api = MpesaAPI()

@router.post("/api/confirmation")
async def confirmation_transaction(request: MpesaRequest):
    try:
        return await mpesa_api.process_confirmation(request)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/api/validation")
async def validate_transaction(request: MpesaRequest):
    try:
        return await mpesa_api.validate_transaction(request)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
