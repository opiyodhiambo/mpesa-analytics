import pykka
import asyncio
from service.etl.load import TransactionLoader

class LoaderActor(pykka.ThreadingActor):
    def __init__(self):
        super().__init__()
        self.transaction_loader = TransactionLoader()