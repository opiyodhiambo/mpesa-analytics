import logging
import pykka

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

class SummaryCalculatorActor(pykka.ThreadingActor):
    def __init__(self):
        super().__init__()
        self.transaction_transformer = TransactionTransformer()

    def on_receive(message):
        """
        Total Transactions and total amount
        """
        logging.info(f"received message :: {message['command']}")
        pass