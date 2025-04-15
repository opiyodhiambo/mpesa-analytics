import logging
import pykka
from service.models.commands import Command
from service.etl.transform import TransactionTransformer
import asyncio

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

        try:
            if command == Command.COMPUTE_TIMESERIES:
                # Handle this async call inside sync context
                event_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(event_loop)

                result = event_loop.run_until_complete(
                    self.transaction_transformer.compute_timeseries(data)
                )
                event_loop.close()
                return result
            elif command == Command.GET_ACTIVITY_HEATMAP:
                return self.transaction_transformer.get_peak_hours(data)
            else:
                logging.warning(f"Unknown command received: {command}")
                return None
        except Exception as e:
            logging.error(f"Error in on_receive: {e}", exc_info=True)
            return {"error": str(e)}

       