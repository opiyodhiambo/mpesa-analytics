import psycopg2
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

    def get_connection(self):
        return psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
        )

    def extract(self, since: str = None) -> pd.DataFrame:
        """
        Here, we extract the mpesa transactions. If `since` is provided, 
        if filters trasnactions that came after that timestamp. 
        """
        query = "SELECT * FROM mpesa_transactions"
        params = ()

        if since:
            query += " WHERE trasnaction_time > %s"
            params = (since,)
        
        with self.get_connection() as connection: # Ensures the connection is properly closed after the block
            df = pd.read_sql_query(query, connection, params=params)

        return df



