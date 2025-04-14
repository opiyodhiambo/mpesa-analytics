from fastapi import APIRouter, HTTPException
from api.models import MpesaRequest
from api.mpesa_api import MpesaAPI

router = APIRouter()

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
