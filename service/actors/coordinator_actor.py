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

        logging.info("CoordinatorActor Spawing child actors ")
        self.transformer_actor = TransformerActor.start()
        self.loader_actor = LoaderActor.start()

    def on_receive(self, message):
        logging.info(f"CoordinatorActor received command: {message['command']}")
        try:
            if message.get("command") == Command.RUN_BATCH:

                # Extracting data 
                raw_data = self.transaction_extractor.extract()

                logging.info("CoordinatorActor Sending messages to child actors")
                transformed_data = self.transformer_actor.ask({"command": Command.TRANSFORM, "data": raw_data})
                result = self.loader_actor.ask({"command": Command.LOAD, "data": transformed_data})
        
                return result
        except Exception as e:
            logging.error(f"Error in on_receive: {e}", exc_info=True)
            return {"error": str(e)}


    
