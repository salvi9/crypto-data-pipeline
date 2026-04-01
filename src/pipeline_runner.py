from datetime import datetime, timedelta
from typing import Dict, List
import os
import sys

CURRENT_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if PROJECT_ROOT not in sys.path:
	sys.path.insert(0, PROJECT_ROOT)

from src.db_connector import DatabaseConnector


def snapshot_stats(db: DatabaseConnector) -> Dict[str, int]:
	"""Return current row counts for all layers."""
	return db.get_pipeline_stats()


def print_stats_delta(before: Dict[str, int], after: Dict[str, int]) -> None:
	"""Print before/after counts and deltas."""
	print("\n=== PIPELINE STATS ===")
	print(f"Before: {before}")
	print(f"After:  {after}")

	print("\n=== DELTAS ===")
	for key in ("bronze", "silver", "gold_hourly", "gold_daily"):
		b = before.get(key, 0)
		a = after.get(key, 0)
		d = a - b
		print(f"{key}: {b} -> {a} (delta: {d:+d})")


def get_sample_bronze_records() -> List[Dict]:
	"""Return a small sample batch with one intentional duplicate."""
	base = datetime.now().replace(microsecond=0, second=0)
	t0 = base.strftime("%Y-%m-%d %H:%M:%S")
	t1 = (base + timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
	t2 = (base + timedelta(minutes=2)).strftime("%Y-%m-%d %H:%M:%S")

	return [
		{
			"symbol": "AAPL",
			"timestamp": t0,
			"open_price": 182.10,
			"high": 183.25,
			"low": 181.80,
			"close": 182.95,
			"volume": 1200000,
		},
		{
			"symbol": "MSFT",
			"timestamp": t1,
			"open_price": 421.00,
			"high": 422.40,
			"low": 420.10,
			"close": 421.75,
			"volume": 980000,
		},
		{
			"symbol": "NVDA",
			"timestamp": t2,
			"open_price": 903.50,
			"high": 910.20,
			"low": 901.75,
			"close": 908.40,
			"volume": 1500000,
		},
		{
			"symbol": "AAPL",
			"timestamp": t0,
			"open_price": 182.10,
			"high": 183.25,
			"low": 181.80,
			"close": 182.95,
			"volume": 1200000,
		},
	]


def ingest_bronze_sample(db: DatabaseConnector) -> Dict[str, int]:
	"""Insert sample rows into Bronze and return counts."""
	records = get_sample_bronze_records()

	inserted = 0
	failed = 0

	print("\n=== BRONZE INGEST ===")

	for record in records:
		success = db.insert_raw_data(
			symbol=record["symbol"],
			timestamp=record["timestamp"],
			open_price=record["open_price"],
			high=record["high"],
			low=record["low"],
			close=record["close"],
			volume=record["volume"],
		)

		if success:
			inserted += 1
			print(f"Inserted Bronze row: {record['symbol']} @ {record['timestamp']}")
		else:
			failed += 1
			print(f"Skipped Bronze row: {record['symbol']} @ {record['timestamp']}")

	summary = {
		"attempted": len(records),
		"inserted": inserted,
		"failed": failed,
	}

	print(f"Bronze summary: {summary}")
	return summary


def assess_quality(record: Dict) -> Dict[str, object]:
	"""Apply basic data-quality checks to one row."""
	issues = []

	open_price = float(record["open"])
	high = float(record["high"])
	low = float(record["low"])
	close = float(record["close"])
	volume = int(record["volume"])

	if open_price <= 0 or high <= 0 or low <= 0 or close <= 0:
		issues.append("non_positive_price")

	if high < low:
		issues.append("high_below_low")

	if volume < 0:
		issues.append("negative_volume")

	is_valid = len(issues) == 0
	score = 1.0 if is_valid else 0.5

	return {
		"is_valid": is_valid,
		"score": score,
		"issues": issues,
	}


def clean_bronze_to_silver(db: DatabaseConnector, limit: int = 100) -> Dict[str, int]:
	"""Move unprocessed Bronze rows into Silver with quality scoring."""
	raw_rows = db.get_raw_data_for_cleaning(limit=limit)

	processed = 0
	inserted = 0
	invalid = 0

	print("\n=== SILVER CLEANING ===")
	print(f"Rows fetched for cleaning: {len(raw_rows)}")

	for row in raw_rows:
		processed += 1
		quality = assess_quality(row)

		success = db.insert_clean_data(
			symbol=row["symbol"],
			timestamp=str(row["timestamp"]),
			open_price=float(row["open"]),
			high=float(row["high"]),
			low=float(row["low"]),
			close=float(row["close"]),
			volume=int(row["volume"]),
			data_quality_score=float(quality["score"]),
			is_valid=bool(quality["is_valid"]),
		)

		if success:
			inserted += 1
			if not quality["is_valid"]:
				invalid += 1
			print(
				f"Cleaned row: {row['symbol']} @ {row['timestamp']} "
				f"(valid={quality['is_valid']}, score={quality['score']})"
			)
		else:
			print(f"Skipped clean insert: {row['symbol']} @ {row['timestamp']}")

	summary = {
		"processed": processed,
		"inserted": inserted,
		"invalid": invalid,
	}

	print(f"Silver summary: {summary}")
	return summary


def aggregate_silver_to_gold(db: DatabaseConnector, limit: int = 1000) -> Dict[str, int]:
	"""Aggregate Silver rows into Gold hourly and daily tables."""
	rows = db.get_cleaned_data_for_metrics(limit=limit)

	print("\n=== GOLD AGGREGATION ===")
	print(f"Rows fetched for metrics: {len(rows)}")

	if not rows:
		summary = {
			"rows_for_metrics": 0,
			"hourly_attempted": 0,
			"hourly_inserted": 0,
			"daily_attempted": 0,
			"daily_inserted": 0,
		}
		print(f"Gold summary: {summary}")
		return summary

	# Build hourly groups.
	hourly_groups = {}
	for row in rows:
		ts = row["timestamp"]
		if not isinstance(ts, datetime):
			ts = datetime.fromisoformat(str(ts))
		hour_start = ts.replace(minute=0, second=0, microsecond=0)
		key = (row["symbol"], hour_start)
		hourly_groups.setdefault(key, []).append({**row, "_ts": ts})

	hourly_attempted = 0
	hourly_inserted = 0

	for (symbol, hour_start), group_rows in hourly_groups.items():
		hourly_attempted += 1
		sorted_rows = sorted(group_rows, key=lambda r: r["_ts"])

		hour_open = float(sorted_rows[0]["open"])
		hour_close = float(sorted_rows[-1]["close"])
		hour_high = max(float(r["high"]) for r in sorted_rows)
		hour_low = min(float(r["low"]) for r in sorted_rows)
		hour_volume = int(sum(int(r["volume"]) for r in sorted_rows))
		avg_price = sum(float(r["close"]) for r in sorted_rows) / len(sorted_rows)

		price_change = hour_close - hour_open
		price_change_pct = (price_change / hour_open * 100.0) if hour_open else 0.0
		volatility = ((hour_high - hour_low) / hour_open * 100.0) if hour_open else 0.0

		success = db.insert_hourly_metrics(
			symbol=symbol,
			period_start=hour_start.strftime("%Y-%m-%d %H:%M:%S"),
			period_end=(hour_start + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S"),
			hour_open=hour_open,
			hour_high=hour_high,
			hour_low=hour_low,
			hour_close=hour_close,
			hour_volume=hour_volume,
			avg_price=avg_price,
			moving_avg_5=None,
			moving_avg_15=None,
			volatility=volatility,
			price_change=price_change,
			price_change_pct=price_change_pct,
			is_high_volatility=volatility > 2.0,
			is_high_volume=hour_volume > 1_000_000,
		)

		if success:
			hourly_inserted += 1
			print(f"Inserted hourly metric: {symbol} @ {hour_start}")
		else:
			print(f"Skipped hourly metric: {symbol} @ {hour_start}")

	# Build daily groups from same input rows.
	daily_groups = {}
	for row in rows:
		ts = row["timestamp"]
		if not isinstance(ts, datetime):
			ts = datetime.fromisoformat(str(ts))
		trading_date = ts.date()
		key = (row["symbol"], trading_date)
		daily_groups.setdefault(key, []).append({**row, "_ts": ts})

	daily_attempted = 0
	daily_inserted = 0

	for (symbol, trading_date), group_rows in daily_groups.items():
		daily_attempted += 1
		sorted_rows = sorted(group_rows, key=lambda r: r["_ts"])

		day_open = float(sorted_rows[0]["open"])
		day_close = float(sorted_rows[-1]["close"])
		day_high = max(float(r["high"]) for r in sorted_rows)
		day_low = min(float(r["low"]) for r in sorted_rows)
		day_volume = int(sum(int(r["volume"]) for r in sorted_rows))

		daily_change = day_close - day_open
		daily_return_pct = (daily_change / day_open * 100.0) if day_open else 0.0
		intraday_range = day_high - day_low

		success = db.insert_daily_aggregate(
			symbol=symbol,
			trading_date=trading_date.strftime("%Y-%m-%d"),
			day_open=day_open,
			day_high=day_high,
			day_low=day_low,
			day_close=day_close,
			day_volume=day_volume,
			daily_change=daily_change,
			daily_return_pct=daily_return_pct,
			upside=max(0.0, day_high - day_open),
			downside=max(0.0, day_open - day_low),
			intraday_range=intraday_range,
		)

		if success:
			daily_inserted += 1
			print(f"Inserted daily aggregate: {symbol} @ {trading_date}")
		else:
			print(f"Skipped daily aggregate: {symbol} @ {trading_date}")

	summary = {
		"rows_for_metrics": len(rows),
		"hourly_attempted": hourly_attempted,
		"hourly_inserted": hourly_inserted,
		"daily_attempted": daily_attempted,
		"daily_inserted": daily_inserted,
	}

	print(f"Gold summary: {summary}")
	return summary


def run_pipeline() -> None:
	"""Run Bronze, Silver, and Gold steps and print summary stats."""
	start = datetime.now()
	print(f"Pipeline started at {start.isoformat(timespec='seconds')}")

	with DatabaseConnector() as db:
		before = snapshot_stats(db)

		bronze_summary = ingest_bronze_sample(db)
		silver_summary = clean_bronze_to_silver(db, limit=100)
		gold_summary = aggregate_silver_to_gold(db, limit=1000)

		after = snapshot_stats(db)

	print(f"\nBronze run summary: {bronze_summary}")
	print(f"Silver run summary: {silver_summary}")
	print(f"Gold run summary: {gold_summary}")

	end = datetime.now()
	print_stats_delta(before, after)
	print(f"\nPipeline finished at {end.isoformat(timespec='seconds')}")
	print(f"Duration: {end - start}")


if __name__ == "__main__":
	run_pipeline()
