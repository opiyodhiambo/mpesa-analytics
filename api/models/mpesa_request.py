class MpesaRequest:
    def __init__(self, data):
        self.transaction_type = data.get("TransactionType")
        self.trans_id = data.get("TransID")
        self.trans_time = data.get("TransTime")
        self.trans_amount = data.get("TransAmount")
        self.business_short_code = data.get("BusinessShortCode")
        self.bill_ref_number = data.get("BillRefNumber")
        self.invoice_number = data.get("InvoiceNumber")
        self.org_account_balance = data.get("OrgAccountBalance")
        self.third_party_trans_id = data.get("ThirdPartyTransID")
        self.msisdn = data.get("MSISDN")
        self.first_name = data.get("FirstName")
        self.middle_name = data.get("MiddleName")
        self.last_name = data.get("LastName")

    
    @classmethod
    def from_request(cls, json_data):
        return cls(json_data)
    
    def __repr__(self):
        return f"<MpesaConfirmationRequest {self.trans_id} - {self.trans_amount}>"