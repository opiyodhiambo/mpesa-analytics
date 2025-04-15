from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from sqlalchemy import create_engine
import asyncio
from pandas import DataFrame
import numpy as np
import pandas as pd

class TransactionTransformer:
    def __init__(self):
        self.process_pool = ProcessPoolExecutor()
        self.thread_pool = ThreadPoolExecutor()
        self.dbname = os.getenv("DATABASE_NAME")
        self.user = os.getenv("DATABASE_USER")
        self.password = os.getenv("DATABASE_PASSWORD")
        self.host = os.getenv("DATABASE_HOST", "localhost")
        self.port = int(os.getenv("DATABASE_PORT", 5432))
        self.db_engine = self._create_engine()


    def parse_time(self, df: DataFrame) -> DataFrame:
        df['transaction_time'] = pd.to_datetime(df['transaction_time'], format='%Y%m%d%H%M%S')
        return df


    async def get_repeat_customers(self, df: pd.DataFrame) -> pd.DataFrame:
        now = pd.Timestamp.now()

        # New metrics from the incoming batch
        new_grouped = df.groupby('msisdn').agg(
            total_transactions=('transaction_id', 'count'),
            total_spend=('transaction_amount', 'sum'),
            avg_spend=('transaction_amount', 'mean'),
            last_seen=('transaction_time', 'max')
        ).reset_index()

        msisdns = new_grouped['msisdn'].tolist()

        # Existing customer records from the DB
        # Offloading it to a background thread so it doesn't block your event loop
        existing_df = await asyncio.to_thread(self._fetch_existing_customers, msisdns) 

        if existing_df.empty:
            return pd.DataFrame()  # No repeat customers found

        # Merging with new data
        merged_df = pd.merge(existing_df, new_grouped, on='msisdn', suffixes=('_old', '_new'))

        # updating cummulative data
        updated_customers = self._update_cummulative_metrics(merged_df)

        return updated_customers


    def cluster_customers_fcm(self, df: DataFrame) -> DataFrame:
        return df


    def predict_customer_lifetime_value(self, df: DataFrame) -> DataFrame:
        return df


    def get_peak_activity(self, df: DataFrame) -> DataFrame:
        # Extracting hour and day of week
        df['hour'] = df['transaction_time'].dt.hour + 1
        df['day_of_week'] = df['transaction_time'].dt.day_name()

        # Creating a new pivot table from the current batch
        new_pivot_table = df.pivot_table(
            index='day_of_week',
            columns='hour',
            values='transaction_id',
            aggfunc='count',
            fill_value=0
        )

        # Loading existing pivor table from the database
        with self.db_engine.connect as conn:
            existing_pivot_table = pd.read_sql_table('peak_activity', conn, index_col='day_of_week')

        # Combining the two pivot tables through unioned indexing
        all_index = existing_pivot_table.index.union(new_pivot_table.index)
        all_columns = existing_pivot_table.columns.union(new_pivot_table.columns)

        existing_pivot_table = existing_pivot_table.reindex(index=all_index, columns=all_columns, fill_value=0)
        new_pivot_table = new_pivot_table.reindex(index=all_index, columns=all_columns, fill_value=0)
        combined_pivot_table = existing_pivot_table.add(new_pivot_table, fill_value=0).astype(int)

        return combined_pivot_table


    def get_total_transactions(self, df: DataFrame) -> int:
        return len(df)    


    def compute_transaction_volume(self, df: DataFrame) -> float:
        return df['transaction_amount'].sum() 


    def compute_weekly_trends(self, df: DataFrame) -> DataFrame:
        return df


    def _update_cummulative_metrics(self, df: DataFrame) -> DataFrame:
        # Updating cumulative metrics
        df['total_transactions'] = df['total_transactions_old'] + merged['total_transactions_new']
        df['total_spend'] = df['total_spend_old'] + merged['total_spend_new']
        df['avg_spend'] = df['total_spend'] / merged['total_transactions']
        df['last_seen'] = df[['last_seen_old', 'last_seen_new']].max(axis=1)

        # Recompute churn and loyalty
        df['days_since_last'] = (now - df['last_seen']).dt.days
        df['is_churned'] = df['days_since_last'] > 30
        df['churn_score'] = df['days_since_last'] / 60.0
        df['churn_score'] = df['churn_score'].clip(0, 1)
        df['loyalty_score'] = np.log1p(df['total_transactions']) * (1 - df['churn_score'])

        updated_customers = df[[
            'msisdn',
            'total_transactions',
            'total_spend',
            'avg_spend',
            'last_seen',
            'days_since_last',
            'is_churned',
            'churn_score',
            'loyalty_score'
        ]].copy()

        return updated_customers.sort_values(by='loyalty_score', ascending=False)
        

    def _fetch_existing_customers(self, msisdns):
        query = text("""
            SELECT msisdn, total_transactions, total_spend, avg_spend, last_seen,
                days_since_last, is_churned, churn_score, loyalty_score
            FROM customers
            WHERE msisdn = ANY(:msisdns)
        """)
        with self.db_engine.connect() as conn:
            return pd.read_sql_query(query, conn, params={"msisdns": msisdns})

    def _create_engine(self):
        db_url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"
        return create_engine(db_url)

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


