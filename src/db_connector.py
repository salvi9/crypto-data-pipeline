"""
================================================================================
Database Connection Manager - PostgreSQL Operations
================================================================================
Purpose:
    Manages connections to PostgreSQL database, handles transactions,
    and provides reusable methods for inserting, querying, and updating data.

Design Pattern:
    Context Manager - automatically opens and closes connections
    when entering/exiting a with block.
"""

import os
import logging
from typing import List, Dict, Optional

import psycopg2
from psycopg2 import sql, Error
from dotenv import load_dotenv


load_dotenv()


# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

def setup_logger(name: str) -> logging.Logger:
    """
    Configure logging for database operations.
    
    Args:
        name (str): Logger name (usually __name__)
    
    Returns:
        logging.Logger: Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Only configure if not already configured
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    
    return logger


logger = setup_logger(__name__)


# ============================================================================
# DATABASE CONNECTION MANAGER CLASS
# ============================================================================

class DatabaseConnector:
    """
    Manages PostgreSQL database connections and operations.
    
    Features:
        - Clear connection lifecycle via context manager
        - Automatic connection cleanup
        - Transaction management
        - Parameterized queries (prevents SQL injection)
        - Comprehensive error handling
        - Logging of all operations
    
    Usage Example:
        with DatabaseConnector() as db:
            db.insert_raw_data(symbol='IBM', price=150.50, ...)
            results = db.query_raw_data(symbol='IBM')
    """
    
    def __init__(self):
        """
        Initialize the database connector.
        
        Loads credentials from environment variables (.env file)
        and prepares connection/cursor attributes.
        """
        # Load PostgreSQL credentials from environment variables
        self.host = os.getenv("POSTGRES_HOST", "localhost")
        self.port = os.getenv("POSTGRES_PORT", "5432")
        self.database = os.getenv("POSTGRES_DB", "crypto_db")
        self.user = os.getenv("POSTGRES_USER", "crypto_user")
        self.password = os.getenv("POSTGRES_PASSWORD")
        
        # Validate that password is set (security check)
        if not self.password:
            raise ValueError(
                "POSTGRES_PASSWORD environment variable not set. "
                "Check your .env file."
            )
        
        self.connection = None
        self.cursor = None
        
        logger.info(
            f"DatabaseConnector initialized for {self.user}@{self.host}:{self.port}/{self.database}"
        )
    
    def _get_connection(self):
        """
        Establish a connection to PostgreSQL.
        
        Returns:
            psycopg2.connection: Active database connection
        
        Raises:
            Error: If connection fails
        """
        try:
            connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            logger.info("Successfully connected to PostgreSQL")
            return connection
        
        except Error as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
    
    def __enter__(self):
        """
        Context manager entry: establish connection.
        
        This is called when entering a 'with' block:
            with DatabaseConnector() as db:
                # __enter__ is called here
        """
        self.connection = self._get_connection()
        self.cursor = self.connection.cursor()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context manager exit: cleanup and close connection.
        
        This is called when exiting a 'with' block (whether success or error):
            with DatabaseConnector() as db:
                db.insert_data(...)
            # __exit__ is called here (connection auto-closes)
        
        Args:
            exc_type: Exception type if an error occurred
            exc_val: Exception value
            exc_tb: Exception traceback
        """
        if exc_type is None:
            # No error - commit changes to database
            self.connection.commit()
            logger.info("Transaction committed successfully")
        else:
            # Error occurred - rollback (undo) changes
            self.connection.rollback()
            logger.error(f"Transaction rolled back due to error: {exc_val}")
        
        # Close cursor and connection
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")
    
    # ========================================================================
    # BRONZE LAYER: RAW DATA OPERATIONS
    # ========================================================================
    
    def insert_raw_data(
        self,
        symbol: str,
        timestamp: str,
        open_price: float,
        high: float,
        low: float,
        close: float,
        volume: int
    ) -> bool:
        """
        Insert raw stock data into stock_data_raw table (Bronze layer).
        
        Args:
            symbol (str): Stock ticker (e.g., 'IBM', 'AAPL')
            timestamp (str): ISO format datetime (e.g., '2026-03-24 09:30:00')
            open_price (float): Opening price
            high (float): Highest price during period
            low (float): Lowest price during period
            close (float): Closing price
            volume (int): Number of shares traded
        
        Returns:
            bool: True if successful, False if insert failed or duplicate detected

        Example:
            success = db.insert_raw_data(
                symbol='IBM',
                timestamp='2026-03-24 09:30:00',
                open_price=150.50,
                high=151.75,
                low=149.25,
                close=150.75,
                volume=1500000
            )
        """
        savepoint_name = "sp_insert_raw"
        try:
            # Keep the transaction usable if this insert fails (e.g., duplicate row).
            self.cursor.execute(f"SAVEPOINT {savepoint_name}")

            # SQL query with %s placeholders (not f-strings - prevents SQL injection)
            query = sql.SQL("""
                INSERT INTO stock_data_raw 
                (symbol, timestamp, "open", high, low, "close", volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """)
            
            # Execute with parameters (safely escaped by psycopg2)
            self.cursor.execute(query, (
                symbol,
                timestamp,
                open_price,
                high,
                low,
                close,
                volume
            ))

            self.cursor.execute(f"RELEASE SAVEPOINT {savepoint_name}")
            
            logger.info(f"Inserted raw data: {symbol} at {timestamp}")
            return True
        
        except Error as e:
            try:
                self.cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                self.cursor.execute(f"RELEASE SAVEPOINT {savepoint_name}")
            except Error:
                self.connection.rollback()

            # Check if it's a UNIQUE constraint violation (duplicate)
            if "unique constraint" in str(e).lower():
                logger.warning(f"Duplicate data (ignored): {symbol} at {timestamp}")
                return False
            else:
                logger.error(f"Error inserting raw data: {e}")
                return False
    
    def query_raw_data(
        self,
        symbol: str,
        limit: int = 100
    ) -> List[Dict]:
        """
        Query raw data for a specific symbol.
        
        Args:
            symbol (str): Stock ticker to query
            limit (int): Maximum number of rows to return (default 100)
        
        Returns:
            List[Dict]: List of dictionaries, one per row
        
        Example:
            records = db.query_raw_data(symbol='IBM', limit=10)
            for record in records:
                print(f"{record['symbol']}: {record['close']}")
        """
        try:
            query = sql.SQL("""
                SELECT id, symbol, timestamp, "open", high, low, "close", volume, created_at
                FROM stock_data_raw
                WHERE symbol = %s
                ORDER BY timestamp DESC
                LIMIT %s
            """)
            
            self.cursor.execute(query, (symbol, limit))
            
            # Fetch column names from cursor description
            columns = [desc[0] for desc in self.cursor.description]
            
            # Convert rows to dictionaries
            rows = self.cursor.fetchall()
            results = [dict(zip(columns, row)) for row in rows]
            
            logger.info(f"Queried {len(results)} rows for {symbol}")
            return results
        
        except Error as e:
            logger.error(f"Error querying raw data: {e}")
            return []
    
    # ========================================================================
    # SILVER LAYER: CLEANED DATA OPERATIONS
    # ========================================================================
    
    def insert_clean_data(
        self,
        symbol: str,
        timestamp: str,
        open_price: float,
        high: float,
        low: float,
        close: float,
        volume: int,
        data_quality_score: float = 1.0,
        is_valid: bool = True
    ) -> bool:
        """
        Insert cleaned/validated data into stock_data_clean table (Silver layer).
        
        Args:
            symbol (str): Stock ticker
            timestamp (str): ISO format datetime
            open_price (float): Opening price
            high (float): High price
            low (float): Low price
            close (float): Closing price
            volume (int): Volume traded
            data_quality_score (float): Quality metric 0.0-1.0 (1.0 = perfect)
            is_valid (bool): Whether record passed all validations
        
        Returns:
            bool: True if successful, False otherwise
        
        Note:
            data_quality_score tracks validation confidence:
            - 1.0: Perfect data, passed all checks
            - 0.8: Minor issues but usable
            - 0.5: Questionable, use with caution
        """
        savepoint_name = "sp_insert_clean"
        try:
            # Keep the transaction usable if this insert fails.
            self.cursor.execute(f"SAVEPOINT {savepoint_name}")

            query = sql.SQL("""
                INSERT INTO stock_data_clean 
                (symbol, timestamp, "open", high, low, "close", volume, 
                 data_quality_score, is_valid)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """)
            
            self.cursor.execute(query, (
                symbol,
                timestamp,
                open_price,
                high,
                low,
                close,
                volume,
                data_quality_score,
                is_valid
            ))

            self.cursor.execute(f"RELEASE SAVEPOINT {savepoint_name}")
            
            logger.info(f"Inserted clean data: {symbol} (quality: {data_quality_score})")
            return True
        
        except Error as e:
            try:
                self.cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                self.cursor.execute(f"RELEASE SAVEPOINT {savepoint_name}")
            except Error:
                self.connection.rollback()

            if "unique constraint" in str(e).lower():
                logger.warning(f"Duplicate clean data: {symbol} at {timestamp}")
                return False
            logger.error(f"Error inserting clean data: {e}")
            return False
    
    def get_raw_data_for_cleaning(self, limit: int = 1000) -> List[Dict]:
        """
        Retrieve unprocessed raw data for cleaning pipeline.
        
        Args:
            limit (int): Maximum rows to retrieve
        
        Returns:
            List[Dict]: Raw records not yet in Silver layer
        
        Logic:
            - Finds raw data that hasn't been moved to Silver table yet
            - Ordered by oldest first (FIFO - First In, First Out)
            - Limits to prevent memory overload
        """
        try:
            query = sql.SQL("""
                SELECT r.id, r.symbol, r.timestamp, r."open", r.high, 
                       r.low, r."close", r.volume, r.created_at
                FROM stock_data_raw r
                LEFT JOIN stock_data_clean c 
                    ON r.symbol = c.symbol AND r.timestamp = c.timestamp
                WHERE c.id IS NULL
                ORDER BY r.created_at ASC
                LIMIT %s
            """)
            
            self.cursor.execute(query, (limit,))
            columns = [desc[0] for desc in self.cursor.description]
            rows = self.cursor.fetchall()
            
            results = [dict(zip(columns, row)) for row in rows]
            logger.info(f"Retrieved {len(results)} rows for cleaning")
            return results
        
        except Error as e:
            logger.error(f"Error retrieving raw data for cleaning: {e}")
            return []
    
    # ========================================================================
    # GOLD LAYER: METRICS OPERATIONS
    # ========================================================================
    
    def insert_hourly_metrics(
        self,
        symbol: str,
        period_start: str,
        period_end: str,
        hour_open: float,
        hour_high: float,
        hour_low: float,
        hour_close: float,
        hour_volume: int,
        avg_price: float,
        moving_avg_5: Optional[float] = None,
        moving_avg_15: Optional[float] = None,
        volatility: float = 0.0,
        price_change: float = 0.0,
        price_change_pct: float = 0.0,
        is_high_volatility: bool = False,
        is_high_volume: bool = False
    ) -> bool:
        """
        Insert hourly metrics into stock_metrics_hourly table (Gold layer).
        
        Args:
            symbol (str): Stock ticker
            period_start (str): Hour start time
            period_end (str): Hour end time
            hour_open/high/low/close (float): OHLC for the hour
            hour_volume (int): Volume during the hour
            avg_price (float): Average price during hour
            moving_avg_5 (Optional[float]): 5-period moving average
            moving_avg_15 (Optional[float]): 15-period moving average
            volatility (float): Hourly volatility metric
            price_change (float): Absolute price change
            price_change_pct (float): Percentage price change
            is_high_volatility (bool): Flag if volatility exceeds threshold
            is_high_volume (bool): Flag if volume exceeds threshold
        
        Returns:
            bool: True if successful
        
        Note:
            Optional fields can be None (fills database NULL values)
        """
        savepoint_name = "sp_insert_hourly"
        try:
            # Keep the transaction usable if this insert fails.
            self.cursor.execute(f"SAVEPOINT {savepoint_name}")

            query = sql.SQL("""
                INSERT INTO stock_metrics_hourly 
                (symbol, period_start, period_end, hour_open, hour_high, 
                 hour_low, hour_close, hour_volume, avg_price, moving_avg_5, 
                 moving_avg_15, volatility, price_change, price_change_pct, 
                 is_high_volatility, is_high_volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """)
            
            self.cursor.execute(query, (
                symbol, period_start, period_end, hour_open, hour_high,
                hour_low, hour_close, hour_volume, avg_price, moving_avg_5,
                moving_avg_15, volatility, price_change, price_change_pct,
                is_high_volatility, is_high_volume
            ))

            self.cursor.execute(f"RELEASE SAVEPOINT {savepoint_name}")
            
            logger.info(f"Inserted hourly metrics: {symbol} ({period_start})")
            return True
        
        except Error as e:
            try:
                self.cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                self.cursor.execute(f"RELEASE SAVEPOINT {savepoint_name}")
            except Error:
                self.connection.rollback()

            if "unique constraint" in str(e).lower():
                logger.warning(f"Duplicate hourly metrics: {symbol} at {period_start}")
                return False
            logger.error(f"Error inserting hourly metrics: {e}")
            return False
    
    def get_cleaned_data_for_metrics(self, limit: int = 1000) -> List[Dict]:
        """
        Retrieve cleaned data not yet processed into metrics.
        
        Args:
            limit (int): Maximum rows to retrieve
        
        Returns:
            List[Dict]: Cleaned records ready for metric calculation

        Logic:
            - Uses symbol + hour bucket matching to determine whether data
              has already been aggregated into stock_metrics_hourly
            - Avoids comparing unrelated surrogate IDs across different tables
        """
        try:
            query = sql.SQL("""
                SELECT c.id, c.symbol, c.timestamp, c."open", c.high,
                       c.low, c."close", c.volume
                FROM stock_data_clean c
                LEFT JOIN stock_metrics_hourly h
                    ON c.symbol = h.symbol
                   AND DATE_TRUNC('hour', c.timestamp) = h.period_start
                WHERE h.id IS NULL
                ORDER BY c.timestamp ASC
                LIMIT %s
            """)
            
            self.cursor.execute(query, (limit,))
            columns = [desc[0] for desc in self.cursor.description]
            rows = self.cursor.fetchall()
            
            results = [dict(zip(columns, row)) for row in rows]
            logger.info(f"Retrieved {len(results)} cleaned records for metrics")
            return results
        
        except Error as e:
            logger.error(f"Error retrieving cleaned data: {e}")
            return []
    
    # ========================================================================
    # GOLD LAYER: DAILY AGGREGATES
    # ========================================================================
    
    def insert_daily_aggregate(
        self,
        symbol: str,
        trading_date: str,
        day_open: float,
        day_high: float,
        day_low: float,
        day_close: float,
        day_volume: int,
        daily_change: float = 0.0,
        daily_return_pct: float = 0.0,
        upside: float = 0.0,
        downside: float = 0.0,
        intraday_range: float = 0.0
    ) -> bool:
        """
        Insert daily aggregated data into stock_data_daily table (Gold layer).
        
        Args:
            symbol (str): Stock ticker
            trading_date (str): Date in YYYY-MM-DD format
            day_open/high/low/close (float): Daily OHLC
            day_volume (int): Total daily volume
            daily_change (float): Close - Open
            daily_return_pct (float): Percentage return for the day
            upside (float): How much above previous close
            downside (float): How much below previous close
            intraday_range (float): High - Low
        
        Returns:
            bool: True if successful
        """
        savepoint_name = "sp_insert_daily"
        try:
            # Keep the transaction usable if this insert fails.
            self.cursor.execute(f"SAVEPOINT {savepoint_name}")

            query = sql.SQL("""
                INSERT INTO stock_data_daily
                (symbol, trading_date, day_open, day_high, day_low, day_close,
                 day_volume, daily_change, daily_return_pct, upside, downside,
                 intraday_range)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """)
            
            self.cursor.execute(query, (
                symbol, trading_date, day_open, day_high, day_low, day_close,
                day_volume, daily_change, daily_return_pct, upside, downside,
                intraday_range
            ))

            self.cursor.execute(f"RELEASE SAVEPOINT {savepoint_name}")
            
            logger.info(f"Inserted daily aggregate: {symbol} on {trading_date}")
            return True
        
        except Error as e:
            try:
                self.cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                self.cursor.execute(f"RELEASE SAVEPOINT {savepoint_name}")
            except Error:
                self.connection.rollback()

            if "unique constraint" in str(e).lower():
                logger.warning(f"Daily aggregate already exists: {symbol} on {trading_date}")
                return False
            logger.error(f"Error inserting daily aggregate: {e}")
            return False
    
    # ========================================================================
    # UTILITY METHODS
    # ========================================================================
    
    def check_connection(self) -> bool:
        """
        Verify database connection is still active.
        
        Returns:
            bool: True if connected and able to query, False otherwise
        """
        try:
            self.cursor.execute("SELECT 1")
            logger.info("Database connection check: OK")
            return True
        except Error as e:
            logger.error(f"Database connection check failed: {e}")
            return False
    
    def get_table_row_count(self, table_name: str) -> int:
        """
        Get total row count for a table.
        
        Args:
            table_name (str): Name of table
        
        Returns:
            int: Number of rows in table
        
        Useful for monitoring pipeline progress:
            bronze_rows = db.get_table_row_count('stock_data_raw')
            silver_rows = db.get_table_row_count('stock_data_clean')
            gold_rows = db.get_table_row_count('stock_metrics_hourly')
        """
        try:
            # Use sql.Identifier to safely escape table name
            query = sql.SQL("SELECT COUNT(*) FROM {}").format(
                sql.Identifier(table_name)
            )
            self.cursor.execute(query)
            count = self.cursor.fetchone()[0]
            logger.info(f"Table {table_name}: {count} rows")
            return count
        except Error as e:
            logger.error(f"Error counting rows in {table_name}: {e}")
            return 0
    
    def get_pipeline_stats(self) -> Dict:
        """
        Get statistics across all medallion layers.
        
        Returns:
            Dict: Row counts and status for each layer
        
        Example output:
            {
                'bronze': 10000,
                'silver': 9950,
                'gold_hourly': 998,
                'gold_daily': 45
            }
        
        Use case:
            Monitoring dashboard, Airflow task monitoring, health checks
        """
        return {
            'bronze': self.get_table_row_count('stock_data_raw'),
            'silver': self.get_table_row_count('stock_data_clean'),
            'gold_hourly': self.get_table_row_count('stock_metrics_hourly'),
            'gold_daily': self.get_table_row_count('stock_data_daily')
        }


# ============================================================================
# EXAMPLE USAGE (Testing)
# ============================================================================

if __name__ == "__main__":
    """
    This block runs ONLY when you execute this file directly:
        python src/db_connector.py
    
    It does NOT run when imported into another module:
        from src.db_connector import DatabaseConnector
    
    This is useful for testing the module independently.
    """
    try:
        # Test basic connection and operations
        with DatabaseConnector() as db:
            # Check connection
            if db.check_connection():
                print("✓ Database connection successful")
            
            # Get current statistics
            stats = db.get_pipeline_stats()
            print(f"Pipeline Statistics: {stats}")
            
            # Test inserting sample data
            success = db.insert_raw_data(
                symbol="TEST",
                timestamp="2026-03-24 09:30:00",
                open_price=100.00,
                high=101.50,
                low=99.50,
                close=100.75,
                volume=1000000
            )
            print(f"Sample insert: {'✓ Success' if success else '✗ Failed'}")
    
    except Exception as e:
        print(f"Error: {e}")