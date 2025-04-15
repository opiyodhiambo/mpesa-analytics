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
        logging.info(f"received message :: {message}")
        if message.get("command") == Command.LOAD:
            transformed_data = message["data"]
            return self.transaction_loader.load(transformed_data)
            