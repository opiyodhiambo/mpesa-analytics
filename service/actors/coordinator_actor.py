import pykka
import logging
from service.actors.transformer_actor import TransformerActor
from service.actors.loader_actor import LoaderActor
from service.etl.extract import TransactionExtractor
from service.models.commands import Command

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

class CoordinatorActor(pykka.ThreadingActor):
    def __init__(self):
        super().__init__()
        self.transaction_extractor = TransactionExtractor()
        self.transformer_actor = TransformerActor.start()
        self.loader_actor = LoaderActor.start()

    def on_receive(self, message):
        logging.info(f"received message :: {message['command']}")
        try:
            if message.get("command") == Command.RUN_BATCH:
                raw_data = self.transaction_extractor.extract()
                
                logging.info(f"Coordinator extracted raw data")
                logging.info(f"sending raw data to: {self.transformer_actor}")

                transformed_data = self.transformer_actor.ask({
                    "command": Command.TRANSFORM,
                    "data": raw_data
                })

                logging.info(f"Coordinator received transformed data from {self.transformer_actor}")
                logging.info(f"sending transformed data to: {self.loader_actor}")

                result = self.loader_actor.ask({
                    "command": Command.LOAD,
                    "data": transformed_data
                })
                logging.info("Coordinator completed load step")
                return result
        except Exception as e:
            logging.error(f"Error in on_receive: {e}", exc_info=True)
            return {"error": str(e)}


    
