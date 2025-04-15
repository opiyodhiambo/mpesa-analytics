import logging
import pykka
from service.models.commands import Command
from service.etl.transform import TransactionTransformer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

class TemporalAnalyzerActor(pykka.ThreadingActor):
    """
    Handles
    - Peak activity Heatmap 
    - Compute weekly trend
    """
    def __init__(self):
        super().__init__()
        self.transaction_transformer = TransactionTransformer()

    def on_receive(self, message):
        command = message.get("command")
        data = message.get("data")

        logging.info(f"TemporalAnalyzerActor received command: {command}")

        if command == Command.COMPUTE_WEEKLY_TRENDS:
            return self.transaction_transformer.compute_weekly_trends(data)
        elif command == Command.GET_ACTIVITY_HEATMAP:
            return self.transaction_transformer.get_peak_activity(data)
        else:
            logging.warning(f"Unknown command received: {command}")
            return None