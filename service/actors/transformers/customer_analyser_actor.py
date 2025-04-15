import logging
import pykka

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

class CustomerAnalyserActor(pykka.ThreadingActor):
    def __init__(self):
        super().__init__()
        self.transaction_transformer = TransactionTransformer()

    def on_receive(message):
        """
        Identify repeat customers 
        Clustering through FCM
        Predict customer lifefime value
        """
        logging.info(f"received message :: {message['command']}")
        pass