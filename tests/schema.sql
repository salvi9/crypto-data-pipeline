-- Schema bootstrap for CI smoke tests

CREATE TABLE IF NOT EXISTS stock_data_raw (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    "open" NUMERIC(12,4) NOT NULL,
    high NUMERIC(12,4) NOT NULL,
    low NUMERIC(12,4) NOT NULL,
    "close" NUMERIC(12,4) NOT NULL,
    volume BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT stock_data_raw_symbol_timestamp_key UNIQUE (symbol, timestamp),
    CONSTRAINT chk_prices_positive CHECK (
        "open" > 0 AND high > 0 AND low > 0 AND "close" > 0 AND volume >= 0
    ),
    CONSTRAINT chk_ohlc_logic CHECK (
        high >= low AND high >= "open" AND high >= "close" AND low <= "open" AND low <= "close"
    )
);

CREATE TABLE IF NOT EXISTS stock_data_clean (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    "open" NUMERIC(12,4) NOT NULL,
    high NUMERIC(12,4) NOT NULL,
    low NUMERIC(12,4) NOT NULL,
    "close" NUMERIC(12,4) NOT NULL,
    volume BIGINT NOT NULL,
    data_quality_score NUMERIC(3,2) NOT NULL DEFAULT 1.0,
    is_valid BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT stock_data_clean_symbol_timestamp_key UNIQUE (symbol, timestamp),
    CONSTRAINT chk_clean_prices_positive CHECK (
        "open" > 0 AND high > 0 AND low > 0 AND "close" > 0 AND volume >= 0
    ),
    CONSTRAINT chk_data_quality_score CHECK (
        data_quality_score >= 0.0 AND data_quality_score <= 1.0
    )
);

CREATE TABLE IF NOT EXISTS stock_metrics_hourly (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    period_start TIMESTAMP NOT NULL,
    period_end TIMESTAMP NOT NULL,
    hour_open NUMERIC(12,4) NOT NULL,
    hour_high NUMERIC(12,4) NOT NULL,
    hour_low NUMERIC(12,4) NOT NULL,
    hour_close NUMERIC(12,4) NOT NULL,
    hour_volume BIGINT NOT NULL,
    avg_price NUMERIC(12,4) NOT NULL,
    moving_avg_5 NUMERIC(12,4),
    moving_avg_15 NUMERIC(12,4),
    volatility NUMERIC(12,4) NOT NULL DEFAULT 0,
    price_change NUMERIC(12,4) NOT NULL DEFAULT 0,
    price_change_pct NUMERIC(12,4) NOT NULL DEFAULT 0,
    is_high_volatility BOOLEAN NOT NULL DEFAULT FALSE,
    is_high_volume BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT stock_metrics_hourly_symbol_period_start_key UNIQUE (symbol, period_start)
);

CREATE TABLE IF NOT EXISTS stock_data_daily (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    trading_date DATE NOT NULL,
    day_open NUMERIC(12,4) NOT NULL,
    day_high NUMERIC(12,4) NOT NULL,
    day_low NUMERIC(12,4) NOT NULL,
    day_close NUMERIC(12,4) NOT NULL,
    day_volume BIGINT NOT NULL,
    daily_change NUMERIC(12,4) NOT NULL DEFAULT 0,
    daily_return_pct NUMERIC(12,4) NOT NULL DEFAULT 0,
    upside NUMERIC(12,4) NOT NULL DEFAULT 0,
    downside NUMERIC(12,4) NOT NULL DEFAULT 0,
    intraday_range NUMERIC(12,4) NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT stock_data_daily_symbol_trading_date_key UNIQUE (symbol, trading_date)
);

CREATE INDEX IF NOT EXISTS idx_raw_symbol_timestamp
ON stock_data_raw (symbol, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_clean_symbol_timestamp
ON stock_data_clean (symbol, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_hourly_symbol_period_start
ON stock_metrics_hourly (symbol, period_start DESC);

CREATE INDEX IF NOT EXISTS idx_daily_symbol_trading_date
ON stock_data_daily (symbol, trading_date DESC);
