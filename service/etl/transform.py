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

    async def transform(self, df: DataFrame) -> dict[str, DataFrame]:
        return self._parse_time(df)
        
        repeat_customer_task = self._run_in_thread(identify_repeat_customers, df)
        summary_task = self._run_in_process(calculate_summary, df)
        clv_task = self._run_in_process(predict_customer_lifetime_value, df)
        clusters_task = self._run_in_process(cluster_customers_fcm, df)
        weekly_trends_task = self._run_in_process(compute_weekly_timeseries, df)
        heatmap_task = self._run_in_process(get_peak_activity, df)

        (
            repeat_customer,
            summary,
            clv,
            clusters,
            weekly_trends,
            heatmap
        ) = await asyncio.gather(
            repeat_customer_task,
            summary_task,
            clv_task,
            clusters_task,
            weekly_trends_task,
            heatmap_task
        )

        return {
            "repeat_customers": repeat_customer,
            "summary": summary,
            "cltv": clv,
            "clusters": clusters,
            "weekly_trend": weekly_trends,
            "heatmap": heatmap
        }

    def parse_time(self, df: DataFrame) -> DataFrame:
        return df

    def identify_repeat_customers(df: DataFrame) -> DataFrame:
        return df

    def cluster_customers_fcm(df: DataFrame) -> DataFrame:
        return df

    def predict_customer_lifetime_value(df: DataFrame) -> DataFrame:
        return df

    def get_peak_activity(df: DataFrame) -> DataFrame:
        return df

    def calculate_summary(df: DataFrame) -> DataFrame:
        return df   

    def compute_weekly_timeseries(df: DataFrame) -> DataFrame:
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


