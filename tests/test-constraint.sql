-- ============================================================================
-- TEST 1: Valid Data Insert (Should SUCCEED)
-- ============================================================================
INSERT INTO stock_data_raw (symbol, timestamp, "open", high, low, "close", volume)
VALUES ('IBM', '2026-03-24 09:30:00', 150.50, 151.75, 149.25, 150.75, 1500000);

-- ============================================================================
-- TEST 2: Duplicate Symbol+Timestamp (Should FAIL - UNIQUE constraint)
-- ============================================================================
INSERT INTO stock_data_raw (symbol, timestamp, "open", high, low, "close", volume)
VALUES ('IBM', '2026-03-24 09:30:00', 150.60, 151.80, 149.20, 150.80, 1510000);
-- Expected: ERROR: duplicate key value violates unique constraint "stock_data_raw_symbol_timestamp_key"

-- ============================================================================
-- TEST 3: Negative Price (Should FAIL - CHECK constraint)
-- ============================================================================
INSERT INTO stock_data_raw (symbol, timestamp, "open", high, low, "close", volume)
VALUES ('AAPL', '2026-03-24 09:30:00', -150.50, 151.75, 149.25, 150.75, 1500000);
-- Expected: ERROR: new row for relation "stock_data_raw" violates check constraint "chk_prices_positive"

-- ============================================================================
-- TEST 4: Invalid OHLC Logic - High < Low (Should FAIL - CHECK constraint)
-- ============================================================================
INSERT INTO stock_data_raw (symbol, timestamp, "open", high, low, "close", volume)
VALUES ('AAPL', '2026-03-24 10:30:00', 150.50, 149.75, 151.25, 150.75, 1500000);
-- Expected: ERROR: new row for relation "stock_data_raw" violates check constraint "chk_ohlc_logic"

-- ============================================================================
-- TEST 5: Valid AAPL Data (Should SUCCEED)
-- ============================================================================
INSERT INTO stock_data_raw (symbol, timestamp, "open", high, low, "close", volume)
VALUES ('AAPL', '2026-03-24 09:30:00', 180.25, 181.50, 179.75, 180.50, 2000000);

-- ============================================================================
-- VERIFICATION: Query data and check indices used
-- ============================================================================
SELECT * FROM stock_data_raw ORDER BY created_at DESC;

-- Check index usage
EXPLAIN ANALYZE 
SELECT * FROM stock_data_raw WHERE symbol = 'IBM' AND timestamp > '2026-03-24'::timestamp;