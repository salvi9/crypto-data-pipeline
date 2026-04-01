import os
import requests
from typing import List, Dict, Tuple

from dotenv import load_dotenv

load_dotenv()

API_URL = "https://www.alphavantage.co/query"


def fetch_daily(symbol: str) -> Tuple[List[Dict], str]:
    """Fetch daily OHLCV records and return (records, status)."""
    api_key = os.getenv("ALPHAVANTAGE_API_KEY")

    if not api_key:
        return [], "missing_api_key"

    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": "compact",
        "apikey": api_key,
    }

    response = requests.get(API_URL, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()

    if "Error Message" in data:
        return [], "api_error"

    if "Information" in data:
        return [], "rate_limited"

    time_series_key = "Time Series (Daily)"

    if time_series_key not in data:
        return [], "no_data"

    records = []
    for date_str, ohlcv in data[time_series_key].items():
        records.append({
            "symbol": symbol,
            "timestamp": f"{date_str} 00:00:00",
            "open_price": float(ohlcv["1. open"]),
            "high": float(ohlcv["2. high"]),
            "low": float(ohlcv["3. low"]),
            "close": float(ohlcv["4. close"]),
            "volume": int(ohlcv["5. volume"]),
        })

    print(f"Fetched {len(records)} records for {symbol}")
    return records, "ok"


if __name__ == "__main__":
    results, status = fetch_daily("AAPL")
    print(f"status={status}")
    for row in results[:3]:
        print(row)
