from etl.extract import TransactionExtractor
from etl.transform import TransactionTransformer
from etl.load import TransactionLoader

class ETLCoordinator:
    def __init__(self):
        self.extractor = TransactionExtractor()
        self.transformer = TransactionTransformer()
        self.loader = TransactionLoader()

    async def run(self):
        raw_data = self.extractor.extract()
        transformed_data = await self.transformer.transform(raw_data)
        self.loader.load(transformed_data)
