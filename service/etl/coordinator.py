from .extract import TransactionExtractor
from .transform import TransactionTransformer
from .load import TransactionLoader

import logging

# Configure logging once at the top
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler()]  # You could add FileHandler here too
)

class ETLCoordinator:
    def __init__(self):
        self.extractor = TransactionExtractor()
        self.transformer = TransactionTransformer()
        self.loader = TransactionLoader()

    async def run(self):
        raw_data = self.extractor.extract()
        logging.info(f"Extracted raw data :: {raw_data}")
        transformed_data = await self.transformer.transform(raw_data)
        logging.info(f"Transformed extracted data :: {transformed_data}")
        self.loader.load(transformed_data)
