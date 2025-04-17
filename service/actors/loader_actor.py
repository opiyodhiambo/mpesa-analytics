import logging
import pykka
import asyncio
from service.etl.load import TransactionLoader
from service.models.commands import Command

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

class LoaderActor(pykka.ThreadingActor):
    def __init__(self):
        super().__init__()
        self.transaction_loader = TransactionLoader()

    def on_receive(self, message):
        logging.info(f"LoaderActor received command: {message['command']}")

        try:
            if message.get("command") == Command.LOAD:
                transformed_data = message["data"]
                event_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(event_loop)
                result = event_loop.run_until_complete(
                    self.transaction_loader.load(transformed_data)
                )
                event_loop.close()
                return result
        except Exception as e:
            logging.error(f"Error in on_receive: {e}", exc_info=True)
            return {"error": str(e)}
            