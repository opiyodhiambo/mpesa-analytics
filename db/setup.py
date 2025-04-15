import logging
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def run_sql_scripts():

    base_dir = os.path.dirname(os.path.abspath(__file__))
    sql_paths = [
        os.path.join(base_dir, "tables", "mpesa_transactions.sql"),
        os.path.join(base_dir, "tables", "customers.sql"),
        os.path.join(base_dir, "tables", "peak_hours.sql")
    ]

    conn = psycopg2.connect(
        dbname=os.getenv("DATABASE_NAME"),
        user=os.getenv("DATABASE_USER"),
        password=os.getenv("DATABASE_PASSWORD"),
        host=os.getenv("DATABASE_HOST", "localhost"),
        port=int(os.getenv("DATABASE_PORT", 5432)),
    )
    cur = conn.cursor()

    for path in sql_paths:
        with open(path, "r") as f:
            sql = f.read()
            cur.execute(sql)
            print(f"Executed {path}")

    conn.commit()
    cur.close()
    conn.close()
    logging.info("Database setup completed.")

