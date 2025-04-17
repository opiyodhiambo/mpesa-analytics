from pandas import DataFrame
import logging
import os
from dotenv import load_dotenv
from typing import Dict, Any
import asyncio
from pandas import DataFrame

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


class TransactionLoader:
    def __init__(self):
        self.dbname = os.getenv("DATABASE_NAME")
        self.user = os.getenv("DATABASE_USER")
        self.password = os.getenv("DATABASE_PASSWORD")
        self.host = os.getenv("DATABASE_HOST", "localhost")
        self.port = int(os.getenv("DATABASE_PORT", 5432))

    def get_connection(self):
        return psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
        )

    async def load(self, data: Dict[str, Any]):
        await asyncio.gather(
            self._load_transaction_metrics(total=data["total_transactions"], volume=data["transaction_volume"]),
            self._load_customers(data["customers"]),
            self._load_trends(data["timeseries_trends"]),
            self._load_activity_heatmap(data["activity_heatmap"]),
        )

    async def _load_transaction_metrics(self, total: int, volume: float):
        await asyncio.to_thread(self._update_metrics, total=total, volume=volume)

    async def _load_customers(self, df: DataFrame): 
        await asyncio.to_thread(self._update_customer_metrics, df)

    async def _load_trends(self, timeseries_trends: Dict[str, Any]):
        await asyncio.to_thread(self._update_trends, timeseries_trends)

    async def _load_activity_heatmap(self, df: DataFrame):
        await asyncio.to_thread(self._update_heatmap, df)

    def _update_metrics(self, total: int, volume: float):
        logging.info("Updating transaction metrics")

    def _update_customer_metrics(self, df: DataFrame):
        logging.info("Updating customer metrics")

    def _update_trends(self, timeseries_trends: Dict[str, Any]):
        logging.info("Updating time series trends")

    def _update_heatmap(self, df: DataFrame):
        logging.info("Updating heat map table")
    

    
