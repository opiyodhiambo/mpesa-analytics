import logging
import pykka
import asyncio
from service.etl.transform import TransactionTransformer
from service.models.commands import Command

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

class TransformerActor(pykka.ThreadingActor):
    def __init__(self):
        super().__init__()
        self.transaction_transformer = TransactionTransformer()

    def on_receive(self, message):
        logging.info(f"received message :: {message}")
        if message.get("command") == Command.TRANSFORM:
            raw_data = message["data"]

            # Since the actor is running in a separate thread, that thread doesn't automatically have an event loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop=loop)
            return loop.run_until_complete(self.transaction_transformer.transform(raw_data))

