import logging
import pykka
from service.models.commands import Command
from service.etl.transform import TransactionTransformer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

class CustomerAnalyserActor(pykka.ThreadingActor):
    """
    Handles
    - Identify repeat customers 
    - Clustering through FCM
    - Predict customer lifefime value
    """
    def __init__(self):
        super().__init__()
        self.transaction_transformer = TransactionTransformer()

    def on_receive(self, message):
        command = message.get("command")
        data = message.get("data")

        logging.info(f"CustomerAnalyserActor received command: {command}")
        try:
            if command == Command.GET_REPEAT_CUSTOMERS:
                return self.transaction_transformer.get_repeat_customers(data)
            elif command == Command.COMPUTE_CLTV:
                return self.transaction_transformer.predict_customer_lifetime_value(data)
            elif command == Command.CLUSTER_CUSTOMERS_FCM:
                return self.transaction_transformer.cluster_customers_fcm(data)
            else:
                logging.warning(f"Unknown command received: {command}")
                return None
        except Exception as e:
            logging.error(f"Error in on_receive: {e}", exc_info=True)
            return {"error": str(e)}

        
