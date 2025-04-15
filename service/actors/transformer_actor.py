import pykka
import asyncio
from service.etl.transform import TransactionTransformer

class TransformerActor(pykka.ThreadingActor):
    def __init__(self):
        super().__init__()
        self.transformer = TransactionTransformer()