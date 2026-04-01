import os
import requests
from typing import List, Dict

from dotenv import load_dotenv

load_dotenv()

API_URL = "https://www.alphavantage.co/query"


def fetch_daily(symbol: str) -> List[Dict]:
    """Fetch daily OHLCV records for a symbol from Alpha Vantage."""
    api_key = os.getenv("ALPHAVANTAGE_API_KEY")

    if not api_key:
        print("ALPHAVANTAGE_API_KEY not set in .env")
        return []

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
        print(f"API error for {symbol}: {data['Error Message']}")
        return []

    if "Information" in data:
        print(f"API rate limit hit: {data['Information']}")
        return []

    time_series_key = "Time Series (Daily)"

    if time_series_key not in data:
        print(f"No data for {symbol}. Response keys: {list(data.keys())}")
        return []

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
    return records


if __name__ == "__main__":
    results = fetch_daily("AAPL")
    for row in results[:3]:
        print(row)
