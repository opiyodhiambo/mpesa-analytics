from pandas import DataFrame
import os
from dotenv import load_dotenv

load_dotenv()

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

    def load(self, df: DataFrame):
        return df