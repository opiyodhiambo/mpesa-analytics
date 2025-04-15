import logging
import pykka

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

class TemporalAnalyzerActor(pykka.ThreadingActor):
    """
    Peak activity Heatmap 
    Compute weekly trend
    """
    def __init__(self):
        super().__init__()
        self.transaction_transformer = TransactionTransformer()

    def on_receive(message):
        logging.info(f"received message :: {message['command']}")
        pass