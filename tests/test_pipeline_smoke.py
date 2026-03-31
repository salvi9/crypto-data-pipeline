from datetime import datetime, timedelta
import os
import sys
import uuid

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.db_connector import DatabaseConnector


def run_pipeline_smoke_test():
    # Keep symbol length under varchar(10)
    run_id = uuid.uuid4().hex[:6].upper()
    symbol = f"S{run_id}"

    base_ts = datetime(2026, 3, 31, 10, 0, 0)
    ts_str = base_ts.strftime("%Y-%m-%d %H:%M:%S")
    hour_start = base_ts.strftime("%Y-%m-%d %H:%M:%S")
    hour_end = (base_ts + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
    trading_date = base_ts.strftime("%Y-%m-%d")

    with DatabaseConnector() as db:
        before = db.get_pipeline_stats()
        print("stats_before =", before)

        raw_ok = db.insert_raw_data(
            symbol=symbol,
            timestamp=ts_str,
            open_price=100.0,
            high=101.5,
            low=99.5,
            close=100.8,
            volume=1_000_000,
        )
        print("bronze_insert =", raw_ok)

        raw_dup = db.insert_raw_data(
            symbol=symbol,
            timestamp=ts_str,
            open_price=100.0,
            high=101.5,
            low=99.5,
            close=100.8,
            volume=1_000_000,
        )
        print("bronze_duplicate =", raw_dup)

        bronze_rows = db.query_raw_data(symbol=symbol, limit=20)
        exact_rows = [r for r in bronze_rows if str(r["timestamp"]).startswith(ts_str)]
        print("bronze_match_count =", len(exact_rows))

        clean_ok = db.insert_clean_data(
            symbol=symbol,
            timestamp=ts_str,
            open_price=100.0,
            high=101.5,
            low=99.5,
            close=100.8,
            volume=1_000_000,
            data_quality_score=1.0,
            is_valid=True,
        )
        print("silver_insert =", clean_ok)

        hourly_ok = db.insert_hourly_metrics(
            symbol=symbol,
            period_start=hour_start,
            period_end=hour_end,
            hour_open=100.0,
            hour_high=101.5,
            hour_low=99.5,
            hour_close=100.8,
            hour_volume=1_000_000,
            avg_price=100.45,
            moving_avg_5=None,
            moving_avg_15=None,
            volatility=0.8,
            price_change=0.8,
            price_change_pct=0.8,
            is_high_volatility=False,
            is_high_volume=False,
        )
        print("gold_hourly_insert =", hourly_ok)

        daily_ok = db.insert_daily_aggregate(
            symbol=symbol,
            trading_date=trading_date,
            day_open=100.0,
            day_high=101.5,
            day_low=99.5,
            day_close=100.8,
            day_volume=1_000_000,
            daily_change=0.8,
            daily_return_pct=0.8,
            upside=0.5,
            downside=0.2,
            intraday_range=2.0,
        )
        print("gold_daily_insert =", daily_ok)

        after = db.get_pipeline_stats()
        print("stats_after =", after)
        print("symbol_used =", symbol)

        assert raw_ok is True, "Expected bronze insert to succeed"
        assert raw_dup is False, "Expected duplicate bronze insert to return False"
        assert len(exact_rows) == 1, "Expected exactly one matching bronze row"
        assert clean_ok is True, "Expected silver insert to succeed"
        assert hourly_ok is True, "Expected gold hourly insert to succeed"
        assert daily_ok is True, "Expected gold daily insert to succeed"

        assert after["bronze"] == before["bronze"] + 1, "Expected bronze count to increase by 1"
        assert after["silver"] == before["silver"] + 1, "Expected silver count to increase by 1"
        assert after["gold_hourly"] == before["gold_hourly"] + 1, "Expected gold_hourly count to increase by 1"
        assert after["gold_daily"] == before["gold_daily"] + 1, "Expected gold_daily count to increase by 1"

        print("smoke_test_status = PASSED")


if __name__ == "__main__":
    run_pipeline_smoke_test()
