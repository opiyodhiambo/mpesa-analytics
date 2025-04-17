from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from sqlalchemy import create_engine, text
import asyncio
from pandas import DataFrame, to_timedelta
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

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
        logging.info("Parsing time")
        df['transaction_time'] = pd.to_datetime(df['transaction_time'], format='%Y%m%d%H%M%S')

        return df


    def get_total_transactions(self, df: DataFrame) -> int:
        logging.info("Computing total trasnactions")

        return len(df)    


    def compute_transaction_volume(self, df: DataFrame) -> float:
        logging.info("Computing transaction volume")

        df['transaction_amount'] = pd.to_numeric(df['transaction_amount'], errors='coerce')
        df = df.dropna(subset=['transaction_amount'])

        return round(df['transaction_amount'].sum(), 2)


    def get_repeat_customers(self, df: pd.DataFrame) -> pd.DataFrame:
        logging.info("Updating customer metrics")

         # Ensuring transaction_amount is numeric
        df['transaction_amount'] = pd.to_numeric(df['transaction_amount'], errors='coerce')
        df = df.dropna(subset=['msisdn', 'transaction_amount'])

        # New metrics from the incoming batch
        new_grouped = df.groupby('msisdn').agg(
            total_transactions=('transaction_id', 'count'),
            total_spend=('transaction_amount', 'sum'),
            avg_spend=('transaction_amount', 'mean'),
            last_seen=('transaction_time', 'max')
        ).reset_index()

        # Existing customer records from the DB
        msisdns = new_grouped['msisdn'].tolist()
        existing_df = self._fetch_existing_customers(msisdns) 

        if existing_df.empty:
            logging.info("No existing customers found — persisting all as new.")
            self._persist_customers(new_grouped)  
            return new_grouped

        # Merging with new data
        merged_df = pd.merge(existing_df, new_grouped, on='msisdn', suffixes=('_old', '_new'))
        updated_customers = self._update_cummulative_metrics(merged_df)

        return updated_customers


    def get_peak_hours(self, df: DataFrame) -> DataFrame:
        logging.info("Getting peak hours")
        # Extracting hour and day of week
        df['hour'] = df['transaction_time'].dt.hour + 1
        df['day_of_week'] = df['transaction_time'].dt.day_name()

        all_hours = list(range(1, 25))

        # Creating a new pivot table from the current batch
        new_pivot_table = df.pivot_table(
            index='day_of_week',
            columns='hour',
            values='transaction_id',
            aggfunc='count',
            fill_value=0
        )

        # Sorting the days of the week from Monday to Sunday and filling missing hours
        new_pivot_table = new_pivot_table.reindex(columns=all_hours, fill_value=0)
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        new_pivot_table = new_pivot_table.reindex(day_order)

        return new_pivot_table


    async def compute_timeseries(self, df: DataFrame) -> dict[str, DataFrame]:
        logging.info(f"Computing time series")
        daily_task = asyncio.to_thread(self._get_timeseries_trends, df, 'D')
        weekly_task = asyncio.to_thread(self._get_timeseries_trends, df, 'W')
        monthly_task = asyncio.to_thread(self._get_timeseries_trends, df, 'M')

        daily, weekly, monthly = await asyncio.gather(daily_task, weekly_task, monthly_task)

        timeseries_result =  {
            'daily_trends': daily,
            'weekly_trends': weekly,
            'monthly_trends': monthly,
        }
    
        return timeseries_result

    
    def cluster_customers_fcm(self, df: DataFrame) -> DataFrame:
        logging.info(f"Clustering customers")

        # Recency: lower days_since_last is better (recently active)
        df['r_score'] = pd.qcut(df['days_since_last'], q=5, labels=[5, 4, 3, 2, 1]).astype(int)
        # Frequency: higher total_transactions is better
        df['f_score'] = pd.qcut(df['total_transactions'].rank(method='first'), q=5, labels=[1, 2, 3, 4, 5]).astype(int)
        # Monetary: higher avg_spend is better
        df['m_score'] = pd.qcut(df['avg_spend'].rank(method='first'), q=5, labels=[1, 2, 3, 4, 5]).astype(int)

        df['customer_segment'] = df.apply(self._assign_segment, axis=1)
    
        return df


    def predict_customer_lifetime_value(self, df: DataFrame) -> DataFrame:
        logging.info("Calculating clv")
        df['clv'] = df.apply(self._calculate_clv, axis=1)
        
        return df


    def _calculate_clv(self, row):
        if row['first_seen'] > row['last_seen']:
            return 0.0

        delta = relativedelta(row['last_seen'], row['first_seen'])
        months = delta.years * 12 + delta.months + (delta.days / 30.44)  # Partial month as a fraction
        months_active = round(months, 2)

        if months_active == 0:
            return 0.0

        frequency = row['total_transactions'] / months_active
        clv = row['avg_spend'] * frequency * months_active

        return clv

    def _update_cummulative_metrics(self, df: DataFrame) -> DataFrame:
        now = pd.Timestamp.now()
        # Updating cumulative metrics
        df['total_transactions'] = df['total_transactions_old'] + df['total_transactions_new']
        df['total_spend'] = df['total_spend_old'] + df['total_spend_new']
        df['avg_spend'] = df['total_spend'] / df['total_transactions']
        df['last_seen'] = df[['last_seen_old', 'last_seen_new']].max(axis=1)
        df['first_seen'] = df['last_seen'] - to_timedelta(
                np.random.randint(10, 181, size=len(df)), unit='D'
            )

        # Recomputing churn and loyalty
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
            'first_seen',
            'last_seen',
            'days_since_last',
            'is_churned',
            'churn_score',
            'loyalty_score'
        ]].copy()

        return updated_customers.sort_values(by='loyalty_score', ascending=False)

    def _assign_segment(self, row):
        if row['r_score'] >= 4 and row['f_score'] >= 4 and row['m_score'] >= 4:
            return 'Best Customers'
        elif row['r_score'] >= 4 and row['f_score'] >= 4:
            return 'Loyal Customers'
        elif row['r_score'] >= 4 and row['f_score'] <= 2:
            return 'Potential Loyalists'
        elif row['r_score'] <= 2 and row['f_score'] <= 2 and row['m_score'] <= 2:
            return 'Lost Customers'
        elif row['r_score'] <= 2 and row['f_score'] <= 3:
            return 'Churn Risk'
        else:
            return 'Other'


    def _get_timeseries_trends(self, df: DataFrame, freq: str) -> DataFrame:
        """
        Here, we timeseries trends grouped by a given frequency.
        
        Args:
            df: DataFrame with 'transaction_time' and 'transaction_amount'.
            freq: Resampling frequency - 'D' (day), 'W' (week), 'M' (month)
            
        Returns:
            DataFrame with date index and aggregated metrics.
        """
        df = df.copy()
        df['transaction_time'] = pd.to_datetime(df.get('transaction_time', pd.Timestamp.now()))
        df['transaction_amount'] = df.get('transaction_amount', 0)
        df['transaction_id'] = df.get('transaction_id', range(len(df)))
        df['transaction_time'] = pd.to_datetime(df['transaction_time'])
        df.set_index('transaction_time', inplace=True)

        trends = df.resample(freq).agg({
            'transaction_id': 'count',
            'transaction_amount': 'sum'
        }).rename(columns={
            'transaction_id': 'total_transactions',
            'transaction_amount': 'total_amount'
        })

        return trends.reset_index()


    def _persist_customers(self, df: pd.DataFrame):
        try:
            # Check if the dataframe is empty
            if df.empty:
                logging.info("No customers to persist.")
                return

            df = self._update_cummulative_metrics(df)

            # Iterate through the DataFrame to either insert or update customers
            with self.db_engine.connect() as conn:
                for index, row in df.iterrows():
                    msisdn = row['msisdn']
                    total_transactions = row['total_transactions']
                    total_spend = row['total_spend']
                    avg_spend = row['avg_spend']
                    first_seen = row['first_seen']
                    last_seen = row['last_seen']
                    days_since_last = row['days_since_last']
                    is_churned = row['is_churned']
                    churn_score = row['churn_score']
                    loyalty_score = row['loyalty_score']
                    
                    # Checking if the msisdn exists in the database
                    existing_customer_query = text("""
                        SELECT COUNT(*) FROM customers WHERE msisdn = :msisdn
                    """)
                    result = conn.execute(existing_customer_query, {"msisdn": msisdn}).scalar()

                    if result > 0:
                        # If customer exists, update the record
                        update_query = text("""
                            UPDATE customers
                            SET total_transactions = :total_transactions,
                                total_spend = :total_spend,
                                avg_spend = :avg_spend,
                                last_seen = :last_seen,
                                days_since_last = :days_since_last
                                is_churned = :is_churned
                                churn_score = :churn_score
                                loyalty_score = :loyalty_score
                            WHERE msisdn = :msisdn
                        """)

                        logging.info(f"customer exists, update query {update_query}")
                        conn.execute(update_query, {
                            "msisdn": msisdn,
                            "total_transactions": total_transactions,
                            "total_spend": total_spend,
                            "avg_spend": avg_spend,
                            "last_seen": last_seen,
                            "days_since_last": days_since_last,
                            "is_churned": is_churned,
                            "churn_score": churn_score,
                            "loyalty_score": loyalty_score
                        })
                    else:
                        # If customer doesn't exist, insert new record
                        insert_query = text("""
                            INSERT INTO customers (
                                msisdn, 
                                total_transactions, 
                                total_spend, 
                                avg_spend, 
                                first_seen,
                                last_seen, 
                                days_since_last,
                                is_churned,
                                churn_score,
                                loyalty_score
                            )
                            VALUES (:msisdn, :total_transactions, :total_spend, :avg_spend, :last_seen, :days_since_last, :is_churned, :churn_score, :loyalty_score)
                        """)

                        conn.execute(insert_query, {
                            "msisdn": msisdn,
                            "total_transactions": total_transactions,
                            "total_spend": total_spend,
                            "avg_spend": avg_spend,
                            "last_seen": last_seen,
                            "first_seen": last_seen,
                            "days_since_last": days_since_last,
                            "is_churned": is_churned,
                            "churn_score": churn_score,
                            "loyalty_score": loyalty_score
                        })
                conn.commit()

            logging.info("Customer data persisted successfully.")

        except Exception as e:
            logging.error(f"Error while persisting customer data: {e}")
            raise


    def _fetch_existing_customers(self, msisdns):
        query = text("""
            SELECT msisdn, total_transactions, total_spend, avg_spend, first_seen, last_seen,
                days_since_last, is_churned, churn_score, loyalty_score
            FROM customers
            WHERE msisdn = ANY(:msisdns)
        """)
        with self.db_engine.connect() as conn:
            return pd.read_sql_query(query, conn, params={"msisdns": msisdns})

    def _create_engine(self):
        db_url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"
        return create_engine(db_url)



