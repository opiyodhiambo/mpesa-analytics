import logging
import pykka
from service.models.commands import Command
from service.etl.transform import TransactionTransformer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

class SummaryCalculatorActor(pykka.ThreadingActor):
    """
    Handles
    - Total Transactions 
    - Total amount
    """
    def __init__(self):
        super().__init__()
        self.transaction_transformer = TransactionTransformer()

    def on_receive(self, message):
        command = message.get("command")
        data = message.get("data")

        logging.info(f"SummaryCalculatorActor received command: {command}")

        try:
            if command == Command.GET_TOTAL_TRANSACTIONS:
                return self.transaction_transformer.get_total_transactions(data)
            elif command == Command.COMPUTE_TRANSACTION_VOLUME:
                return self.transaction_transformer.compute_transaction_volume(data)
            else:
                logging.warning(f"Unknown command received: {command}")
                return None
        except Exception as e:
            logging.error(f"Error in on_receive: {e}", exc_info=True)
            return {"error": str(e)}