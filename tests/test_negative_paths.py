from datetime import datetime
import uuid
import os
import sys

CURRENT_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.db_connector import DatabaseConnector


def run_negative_path_test():
    run_id = uuid.uuid4().hex[:6].upper()
    symbol = f"N{run_id}"  # <= 10 chars with current schema
    ts = datetime(2026, 3, 31, 11, 0, 0).strftime("%Y-%m-%d %H:%M:%S")

    with DatabaseConnector() as db:
        before = db.get_table_row_count("stock_data_raw")
        print("raw_before =", before)

        # Case A: negative price -> should fail
        neg_price = db.insert_raw_data(
            symbol=symbol,
            timestamp="2026-03-31 11:01:00",
            open_price=-1.0,
            high=101.0,
            low=99.0,
            close=100.0,
            volume=1000,
        )
        print("negative_price_insert =", neg_price)
        assert neg_price is False, "Negative price should be rejected"

        # Case B: high < low -> should fail
        bad_ohlc = db.insert_raw_data(
            symbol=symbol,
            timestamp="2026-03-31 11:02:00",
            open_price=100.0,
            high=98.0,
            low=99.0,
            close=100.0,
            volume=1000,
        )
        print("bad_ohlc_insert =", bad_ohlc)
        assert bad_ohlc is False, "Invalid OHLC should be rejected"

        mid = db.get_table_row_count("stock_data_raw")
        print("raw_after_invalid =", mid)
        assert mid == before, "Invalid rows should not increase raw count"

        # Case C: duplicate symbol+timestamp -> second insert should fail
        first = db.insert_raw_data(
            symbol=symbol,
            timestamp=ts,
            open_price=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=1000,
        )
        second = db.insert_raw_data(
            symbol=symbol,
            timestamp=ts,
            open_price=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=1000,
        )
        print("duplicate_first_insert =", first)
        print("duplicate_second_insert =", second)

        assert first is True, "First insert should succeed"
        assert second is False, "Duplicate insert should be rejected"

        after = db.get_table_row_count("stock_data_raw")
        print("raw_after_all =", after)
        assert after == before + 1, "Only one valid row should be added"

    print("negative_path_test_status = PASSED")


if __name__ == "__main__":
    run_negative_path_test()