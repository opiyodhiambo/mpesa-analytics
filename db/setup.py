import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

def run_sql_script(path: str):
    with open(path, "r") as f:
        sql = f.read()

    conn = psycopg2.connect(
        dbname = os.getenv("DATABASE_NAME"),
        user = os.getenv("DATABASE_USER"),
        password = os.getenv("DATABASE_PASSWORD"),
        host = os.getenv("DATABASE_HOST"),
        port = int(os.getenv("DATABASE_PORT")),
    )
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()
    print("Database setup completed.")

if __name__ == "__main__":
    run_sql_script("schema.sql")
