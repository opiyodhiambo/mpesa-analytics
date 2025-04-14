from api.models import MpesaRequest
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

class MpesaAPI:

    async def process_confirmation(self, confirmation_data: MpesaRequest):
        self.save_transaction(confirmation_data)
        return {"status": "confirmed"}

    async def validate_transaction(self, validation_data: MpesaRequest):
        print("M-Pesa Validation Data:", validation_data)
        return {"status": "validated"}

    def save_transaction(self, data: MpesaRequest):
        db_connection = psycopg2.connect(
            dbname=os.getenv('DATABASE_NAME'),
            user=os.getenv('DATABASE_USER'),
            password=os.getenv('DATABASE_PASSWORD'),
            host='localhost',
            port=5432
        )
        cur = db_connection.cursor()

        cur.execute("""
            INSERT INTO mpesa_transactions (
                transaction_type, 
                transaction_id, 
                transaction_time, 
                transaction_amount, 
                business_short_code, 
                bill_ref_number, 
                invoice_number, 
                org_account_balance,
                third_party_tansaaction_id,
                msisdn,
                first_name,
                middle_name,
                last_name
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            data.TransactionType, 
            data.TransID, 
            data.TransTime, 
            data.TransAmount,
            data.BusinessShortCode, 
            data.BillRefNumber, 
            data.InvoiceNumber,
            data.OrgAccountBalance, 
            data.ThirdPartyTransID,
            data.MSISDN, 
            data.FirstName, 
            data.MiddleName, 
            data.LastName
        ))

        db_connection.commit()
        cur.close()
        db_connection.close()
