import pykka
from service.etl.extract import TransactionExtractor

class CoordinatorActor(pykka.ThreadingActor):
    def __init__(self):
        super().__init__()
        self.transaction_extractor = TransactionExtractor()
        self.transformer_actor = TransformerActor.start()
        self.loader_actor = LoaderActor.start()
    
