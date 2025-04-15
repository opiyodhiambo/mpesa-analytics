from sqlalchemy import create_engine
from dotenv import load_dotenv
import pandas as pd
import os

load_dotenv()

class TransactionExtractor:
    def __init__(self):
        self.dbname = os.getenv("DATABASE_NAME")
        self.user = os.getenv("DATABASE_USER")
        self.password = os.getenv("DATABASE_PASSWORD")
        self.host = os.getenv("DATABASE_HOST", "localhost")
        self.port = int(os.getenv("DATABASE_PORT", 5432))
        self.engine = self.create_engine()

    def create_engine(self):
        db_url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"
        return create_engine(db_url)

    def extract(self, since: str = None) -> pd.DataFrame:
        query = "SELECT * FROM mpesa_transactions"
        params = {}

        if since:
            query += " WHERE transaction_time > %(since)s"
            params = {"since": since}
        
        df = pd.read_sql_query(query, self.engine, params=params)
        return df
