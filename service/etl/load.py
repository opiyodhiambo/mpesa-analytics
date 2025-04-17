from pandas import DataFrame
import logging
import os
import psycopg2
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

        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            # Checking if any row exists
            cursor.execute("SELECT id FROM transaction_metrics LIMIT 1")
            result = cursor.fetchone()

            if result:
                # We update the existing row
                cursor.execute("""
                    UPDATE transaction_metrics
                    SET total_transactions = total_transactions + %s,
                        transaction_volume = transaction_volume + %s
                    WHERE id = %s
                """, (total, volume, result[0]))
            else:
                # We insert the first row
                cursor.execute("""
                    INSERT INTO transaction_metrics (total_transactions, transaction_volume)
                    VALUES (%s, %s)
                """, (total, volume))

            conn.commit()
            logging.info("Transaction metrics updated successfully")

        except Exception as e:
            logging.error(f"Error updating transaction metrics: {e}", exc_info=True)
            conn.rollback()
        finally:
            cursor.close()
            conn.close()


    def _update_customer_metrics(self, df: DataFrame):
        logging.info("Updating customer metrics")
        
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            for _, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO customers (
                        msisdn, total_transactions, total_spend, avg_spend, last_seen,
                        days_since_last, is_churned, churn_score, loyalty_score,
                        first_seen, clv, r_score, f_score, m_score, customer_segment
                    ) VALUES (
                        %(msisdn)s, %(total_transactions)s, %(total_spend)s, %(avg_spend)s, %(last_seen)s,
                        %(days_since_last)s, %(is_churned)s, %(churn_score)s, %(loyalty_score)s,
                        %(first_seen)s, %(clv)s, %(r_score)s, %(f_score)s, %(m_score)s, %(customer_segment)s
                    )
                    ON CONFLICT (msisdn) DO UPDATE SET
                        total_transactions = EXCLUDED.total_transactions,
                        total_spend = EXCLUDED.total_spend,
                        avg_spend = EXCLUDED.avg_spend,
                        last_seen = EXCLUDED.last_seen,
                        days_since_last = EXCLUDED.days_since_last,
                        is_churned = EXCLUDED.is_churned,
                        churn_score = EXCLUDED.churn_score,
                        loyalty_score = EXCLUDED.loyalty_score,
                        first_seen = EXCLUDED.first_seen,
                        clv = EXCLUDED.clv,
                        r_score = EXCLUDED.r_score,
                        f_score = EXCLUDED.f_score,
                        m_score = EXCLUDED.m_score,
                        customer_segment = EXCLUDED.customer_segment
                """, row.to_dict())

            conn.commit()
            logging.info("Customer metrics updated successfully")

        except Exception as e:
            logging.error(f"Error updating customer metrics: {e}", exc_info=True)
            conn.rollback()
        finally:
            cursor.close()
            conn.close()


    def _update_trends(self, timeseries_trends: Dict[str, DataFrame]):
        logging.info("Updating time series trends")

        conn = self.get_connection()
        cursor = conn.cursor()

        for table_name, df in timeseries_trends.items():
            for _, row in df.iterrows():
                transaction_time = row["transaction_time"]
                total_transactions = row["total_transactions"]
                total_amount = row["total_amount"]

                try:
                    cursor.execute(f"""
                        INSERT INTO {table_name} (transaction_time, total_transactions, total_amount)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (transaction_time) DO UPDATE SET
                            total_transactions = EXCLUDED.total_transactions,
                            total_amount = EXCLUDED.total_amount;
                    """, (transaction_time, total_transactions, total_amount))
                except Exception as e:
                    logging.error(f"Failed to insert into {table_name}: {e}", exc_info=True)
                    continue

        conn.commit()
        cursor.close()
        conn.close()



    def _update_heatmap(self, df: DataFrame):
        logging.info(f"Updating heat map table")
        
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            for day_of_week, row in df.iterrows():  
                row_data = row.to_dict()
                update_values = []
                set_clauses = []
                
                # We only include non-zero values in the update
                for hour, value in row_data.items():
                    if value != 0:
                        set_clauses.append(f'"{hour}" = peak_hours."{hour}" + {int(value)}')
                
                if not set_clauses:  # We skip if all values are zero
                    continue
                    
                update_query = f"""
                    UPDATE peak_hours 
                    SET {', '.join(set_clauses)}
                    WHERE day_of_week = %s
                """
                
                cursor.execute(update_query, (day_of_week,))
                
            conn.commit()
            logging.info("Heatmap table updated successfully.")

        except Exception as e:
            logging.error(f"Error updating heatmap table: {e}", exc_info=True)
            conn.rollback()
        finally:
            cursor.close()
            conn.close()