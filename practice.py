import os
import psycopg2
from psycopg2 import Error
from dotenv import load_dotenv

load_dotenv()


class DatabaseConnector:
    
    def __init__(self):
        self.host = os.getenv("POSTGRES_HOST", "localhost")
        self.port = os.getenv("POSTGRES_PORT", "5432")
        self.database = os.getenv("POSTGRES_DB", "crypto_db")
        self.user = os.getenv("POSTGRES_USER", "crypto_user")
        self.password = os.getenv("POSTGRES_PASSWORD")

        if not self.password:
            raise ValueError("POSTGRES_PASSWORD environment variable not set.")

        self.connection = None
        self.cursor = None
        
    def _get_connection(self):
        try:
            connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            return connection
        except Error as e:
            raise
        
    def __enter__(self):
        self.connection = self._get_connection()
        self.cursor = self.connection.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.connection.commit()
        else:
            self.connection.rollback()

        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            
    def insert_raw_data(
        self,
        symbol,
        timestamp,
        open_price,
        high,
        low,
        close,
        volume
    ):
        try:
            query = """
                INSERT INTO stock_data_raw
                (symbol, timestamp, "open", high, low, "close", volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            self.cursor.execute(query, (
                symbol,
                timestamp,
                open_price,
                high,
                low,
                close,
                volume
            ))
            
            return True

        except Error as e:
            if "unique constraint" in str(e).lower():
                return False
            else:
                return False
            
    def query_raw_data(self, symbol, limit=100):
        try:
            query = """
                SELECT id, symbol, timestamp, "open", high, low, "close", volume, created_at
                FROM stock_data_raw
                WHERE symbol = %s
                ORDER BY timestamp DESC
                LIMIT %s
            """
            self.cursor.execute(query, (symbol, limit))

            columns = [desc[0] for desc in self.cursor.description]
            rows = self.cursor.fetchall()
            results = [dict(zip(columns, row)) for row in rows]

            return results
        except Error:
            return []
        
if __name__ == "__main__":
    print("Starting script")
    with DatabaseConnector() as db:
        db.cursor.execute("SELECT 1;")
        result = db.cursor.fetchone()
        print("Query result:", result)
    print("Finished script")