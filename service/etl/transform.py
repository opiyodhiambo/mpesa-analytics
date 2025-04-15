from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import asyncio
from pandas import DataFrame

from .utils.summary_calculator import calculate_summary
from .utils.clv_predictor import predict_customer_lifetime_value
from .utils.customer_analyser import cluster_customers_fcm, identify_repeat_customers
from .utils.weekly_trend_calculator import compute_weekly_timeseries
from .utils.peak_activity_getter import get_peak_activity

class TransactionTransformer:
    def __init__(self):
        self.process_pool = ProcessPoolExecutor()
        self.thread_pool = ThreadPoolExecutor()

    def parse_time(self, df: DataFrame) -> DataFrame:
        return df

    def get_repeat_customers(df: DataFrame) -> DataFrame:
        return df

    def cluster_customers_fcm(df: DataFrame) -> DataFrame:
        return df

    def predict_customer_lifetime_value(df: DataFrame) -> DataFrame:
        return df

    def get_peak_activity(df: DataFrame) -> DataFrame:
        return df

    def get_total_transactions(df: DataFrame) -> DataFrame:
        return df   

    def compute_transaction_volume(df: DataFrame) -> DataFrame:
        return df 

    def compute_weekly_trends(df: DataFrame) -> DataFrame:
        return df

    # For process heavy workloads
    async def _run_in_process(self, func, *args):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.process_pool, func, *args)

    # For Thread heavy workloads
    async def _run_in_thread(self, func, *args):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.thread_pool, func, *args)

    def close(self):
        self.process_pool.shutdown()


