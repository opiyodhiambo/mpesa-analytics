from flask import Flask, request, jsonify
from api.models import MpesaRequest

app = Flask(__name__)
mpesa_api = MpesaAPI()

# Confirmation Endpoint
@app.route("/api/confirmation", methods=["POST"])
async def confirmation_transaction():
    try:
        confirmation_data = MpesaRequest.from_request(request.json)
        return await mpesa_api.process_confirmation(confirmation_data)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

# Validation Endpoint
@app.route('/api/validation', methods=['POST'])
async def validate_transaction():
    try:
        validation_data = MpesaRequest.from_request(request.json)
        return await mpesa_api.validate_transaction(validation_data)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    

