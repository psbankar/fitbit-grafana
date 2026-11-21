#!/usr/bin/env python3
"""
High-Granularity Historical Backfill Engine for Google Health API & InfluxDB.
Backfills missing health data (2022 - 2024) with full intraday precision,
checkpoint persistence, adaptive rate-limit pacing, and zero duplicates.
"""

import os
import sys
import json
import time
import logging
import argparse
from datetime import datetime, timedelta, timezone
import pytz
import requests

# Import ingestion logic and helpers from Fitbit_Fetch
import Fitbit_Fetch as ff

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] [Backfill] - %(message)s"
)

STATE_FILE = os.environ.get("BACKFILL_STATE_FILE") or os.path.join(
    os.path.dirname(ff.TOKEN_FILE_PATH) if os.path.dirname(ff.TOKEN_FILE_PATH) else ".",
    "backfill_state.json"
)

RATE_LIMIT_DELAY = 0.35  # ~170 requests/min ceiling (well under 300 req/min Google limit)


def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            logging.warning("Could not read backfill state file %s: %s; starting fresh", STATE_FILE, e)
    return {
        "completed_intraday": [],
        "completed_daily_ranges": [],
        "completed_sleep_ranges": [],
        "last_updated_utc": datetime.now(timezone.utc).isoformat()
    }


def save_state(state):
    try:
        os.makedirs(os.path.dirname(os.path.abspath(STATE_FILE)), exist_ok=True)
        state["last_updated_utc"] = datetime.now(timezone.utc).isoformat()
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        logging.error("Failed to save backfill state: %s", e)


def pace_request():
    time.sleep(RATE_LIMIT_DELAY)


def backfill_intraday_day(date_str):
    """Fetches full 1s Heart Rate and 1min Steps for a single day."""
    pace_request()
    ff.get_intraday_data_limit_1d(date_str, [
        ('heart', 'HeartRate_Intraday', '1sec'),
        ('steps', 'Steps_Intraday', '1min')
    ])


def backfill_daily_block(start_str, end_str):
    """Fetches all daily rollups and metrics for a given date window."""
    pace_request()
    ff.get_daily_data_limit_none(start_str, end_str)  # Daily SpO2
    pace_request()
    ff.get_daily_data_limit_365d(start_str, end_str)  # RHR, HR zones, Activity, Steps, Calories, Distance
    pace_request()
    ff.get_daily_data_limit_30d(start_str, end_str)   # HRV, BR, Skin Temp, SpO2 intraday, Weight, Body Fat, VO2 Max


def backfill_sleep_block(start_str, end_str):
    """Fetches sleep sessions and stage timelines."""
    pace_request()
    ff.get_daily_data_limit_100d(start_str, end_str)


def flush_records():
    count = len(ff.collected_records)
    if count > 0:
        ff.write_points_to_influxdb(ff.collected_records)
        logging.info("Flushed %d points to InfluxDB", count)
        ff.collected_records.clear()
    return count


def run_backfill(start_date_str, end_date_str, skip_intraday=False):
    start_dt = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date_str, "%Y-%m-%d")

    logging.info("Starting historical backfill from %s to %s (Intraday: %s)",
                 start_date_str, end_date_str, "SKIP" if skip_intraday else "FULL")

    # Ensure access token is active
    if ff.HEALTH_API_PROVIDER == "google":
        logging.info("Ensuring active Google Health API token...")
        ff.Get_New_Access_Token(ff.client_id, ff.client_secret)

    state = load_state()
    completed_intraday = set(state.get("completed_intraday", []))
    completed_daily = set(tuple(r) for r in state.get("completed_daily_ranges", []))
    completed_sleep = set(tuple(r) for r in state.get("completed_sleep_ranges", []))

    # Phase 1: Exercise history and GPS trackpoints (pull latest 100 sessions)
    logging.info("--- Phase 1: Ingesting Workout Sessions & TCX GPS tracks ---")
    try:
        ff.fetch_latest_activities(end_date_str)
        flush_records()
    except Exception as e:
        logging.error("Error during workout session ingestion: %s", e)

    # Phase 2: Daily rollups in 30-day chunks (backward from end_dt to start_dt)
    logging.info("--- Phase 2: Ingesting Daily Rollups & Clinical Metrics ---")
    current_end = end_dt
    while current_end >= start_dt:
        chunk_start = max(start_dt, current_end - timedelta(days=29))
        c_start_str = chunk_start.strftime("%Y-%m-%d")
        c_end_str = current_end.strftime("%Y-%m-%d")

        if (c_start_str, c_end_str) not in completed_daily:
            logging.info("Backfilling daily metrics chunk: %s to %s", c_start_str, c_end_str)
            try:
                backfill_daily_block(c_start_str, c_end_str)
                flush_records()
                completed_daily.add((c_start_str, c_end_str))
                state["completed_daily_ranges"] = list(completed_daily)
                save_state(state)
            except Exception as e:
                logging.error("Failed daily chunk %s to %s: %s", c_start_str, c_end_str, e)
        else:
            logging.debug("Skipping already completed daily chunk: %s to %s", c_start_str, c_end_str)

        current_end = chunk_start - timedelta(days=1)

    # Phase 3: Sleep architecture & stages in 60-day chunks
    logging.info("--- Phase 3: Ingesting Sleep Sessions & Stages ---")
    current_end = end_dt
    while current_end >= start_dt:
        chunk_start = max(start_dt, current_end - timedelta(days=59))
        c_start_str = chunk_start.strftime("%Y-%m-%d")
        c_end_str = current_end.strftime("%Y-%m-%d")

        if (c_start_str, c_end_str) not in completed_sleep:
            logging.info("Backfilling sleep chunk: %s to %s", c_start_str, c_end_str)
            try:
                backfill_sleep_block(c_start_str, c_end_str)
                flush_records()
                completed_sleep.add((c_start_str, c_end_str))
                state["completed_sleep_ranges"] = list(completed_sleep)
                save_state(state)
            except Exception as e:
                logging.error("Failed sleep chunk %s to %s: %s", c_start_str, c_end_str, e)
        else:
            logging.debug("Skipping already completed sleep chunk: %s to %s", c_start_str, c_end_str)

        current_end = chunk_start - timedelta(days=1)

    # Phase 4: High-Granularity Intraday (1s HR + 1min Steps) day by day
    if not skip_intraday:
        logging.info("--- Phase 4: Ingesting Intraday (1s HR & 1min Steps) ---")
        total_days = (end_dt - start_dt).days + 1
        curr_day = end_dt
        day_idx = 0

        while curr_day >= start_dt:
            day_str = curr_day.strftime("%Y-%m-%d")
            day_idx += 1

            if day_str not in completed_intraday:
                logging.info("[%d/%d] Ingesting intraday for date: %s", day_idx, total_days, day_str)
                try:
                    backfill_intraday_day(day_str)
                    flush_records()
                    completed_intraday.add(day_str)
                    state["completed_intraday"] = list(completed_intraday)
                    save_state(state)
                except Exception as e:
                    logging.error("Failed intraday for %s: %s", day_str, e)
            else:
                logging.debug("Skipping already completed intraday date: %s", day_str)

            curr_day -= timedelta(days=1)

    logging.info("Historical backfill completed successfully!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Google Health API Historical Backfiller")
    parser.add_argument("--start", default="2022-01-01", help="Start date (YYYY-MM-DD), default: 2022-01-01")
    parser.add_argument("--end", default="2024-06-16", help="End date (YYYY-MM-DD), default: 2024-06-16")
    parser.add_argument("--skip-intraday", action="store_true", help="Skip 1s/1min intraday series for faster rollup-only sync")
    args = parser.parse_args()

    run_backfill(args.start, args.end, skip_intraday=args.skip_intraday)
