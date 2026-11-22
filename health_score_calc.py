#!/usr/bin/env python3
"""
health_score_calc.py
Boreas Biometric Health & Readiness Engine:
Computes and persists composite daily scores into InfluxDB:
  - Objective Sleep Architecture Score (OSAS) (0-100)
  - Daily Readiness Score (0-100)
  - Autonomic Stress Score (0-100)
  - Biological Fitness Age & Functional VO2 Max (ACSM & CERG models)
"""

import os
import sys
import json
import math
import logging
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta

# Logging configuration
logger = logging.getLogger("health_score_calc")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s - [%(levelname)s] [HealthScoreEngine] - %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

INFLUX_HOST = os.environ.get("INFLUXDB_HOST", "172.25.0.2")
INFLUX_PORT = int(os.environ.get("INFLUXDB_PORT", "8086"))
INFLUX_DB = os.environ.get("INFLUXDB_DATABASE", "fitbit")

def clamp(val, low, high):
    return max(low, min(high, val))

def get_influx_urls():
    base = f"http://{INFLUX_HOST}:{INFLUX_PORT}"
    query_url = f"{base}/query?db={INFLUX_DB}"
    write_url = f"{base}/write?db={INFLUX_DB}"
    return query_url, write_url

def query_influx(q):
    query_url, _ = get_influx_urls()
    data = urllib.parse.urlencode({"q": q}).encode("utf-8")
    try:
        req = urllib.request.Request(query_url, data=data, method="POST")
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data.get("results", [])
    except Exception as e:
        logger.error("Error querying InfluxDB: %s", e)
        return []

def write_influx_lines(lines):
    if not lines:
        return True
    _, write_url = get_influx_urls()
    payload = "\n".join(lines).encode("utf-8")
    try:
        req = urllib.request.Request(write_url, data=payload, headers={"Content-Type": "text/plain"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.status in (200, 204):
                return True
            else:
                logger.error("InfluxDB write returned status %d", resp.status)
                return False
    except Exception as e:
        logger.error("Failed to write points to InfluxDB: %s", e)
        return False

# =============================================================================
# Mathematical Algorithms
# =============================================================================

def calculate_osas(total_asleep_mins, deep_mins, rem_mins, light_mins, awake_mins):
    """
    Objective Sleep Architecture Score (OSAS) (0 - 100)
    Duration: 45 pts (target 7.5 hrs)
    Restorative: 35 pts (Deep + REM target >= 38% of total asleep)
    Continuity: 20 pts (WASO penalty above 20 min)
    """
    total_sleep_hours = total_asleep_mins / 60.0
    restorative_mins = deep_mins + rem_mins
    restorative_ratio = (restorative_mins / max(1.0, total_asleep_mins)) * 100.0

    duration_score = clamp((total_sleep_hours / 7.5) * 45.0, 0.0, 45.0)
    restorative_score = clamp((restorative_ratio / 38.0) * 35.0, 0.0, 35.0)
    continuity_score = clamp(20.0 - max(0.0, awake_mins - 20.0) * 0.4, 0.0, 20.0)
    osas_score = int(clamp(duration_score + restorative_score + continuity_score, 0.0, 100.0))

    return {
        "osas_score": osas_score,
        "total_sleep_hours": round(total_sleep_hours, 2),
        "restorative_ratio": round(restorative_ratio, 1),
        "duration_score": round(duration_score, 1),
        "restorative_score": round(restorative_score, 1),
        "continuity_score": round(continuity_score, 1)
    }

def calculate_readiness(latest_hrv, baseline_hrv, hrv_std, latest_rhr, baseline_rhr, osas_score):
    """
    Daily Readiness Score (0 - 100)
    HRV Recovery: 40%
    Resting HR: 30%
    Sleep Architecture: 30%
    """
    hrv_diff = latest_hrv - baseline_hrv
    hrv_score = clamp(50.0 + (hrv_diff / (2.0 * max(1.0, hrv_std))) * 50.0, 0.0, 100.0)

    rhr_diff = baseline_rhr - latest_rhr
    rhr_score = clamp(50.0 + (rhr_diff / 4.0) * 50.0, 0.0, 100.0)

    readiness = int(clamp(0.40 * hrv_score + 0.30 * rhr_score + 0.30 * osas_score, 0.0, 100.0))

    if readiness >= 80:
        readiness_label = "Optimal"
    elif readiness >= 65:
        readiness_label = "Good"
    elif readiness >= 50:
        readiness_label = "Moderate"
    else:
        readiness_label = "Recovery"

    return {
        "readiness_score": readiness,
        "hrv_score": round(hrv_score, 1),
        "rhr_score": round(rhr_score, 1),
        "readiness_label": readiness_label
    }

def calculate_stress(latest_hrv, baseline_hrv, latest_rhr, baseline_rhr, osas_score):
    """
    Autonomic Stress Score (0 - 100)
    Autonomic Strain: 40% (HRV suppression)
    Hemodynamic Strain: 30% (RHR elevation)
    Sleep Debt: 30% (Inverse of OSAS)
    """
    hrv_suppression_pct = max(0.0, (baseline_hrv - latest_hrv) / max(1.0, baseline_hrv)) * 100.0
    autonomic_strain = clamp(hrv_suppression_pct * 2.5, 0.0, 100.0)

    rhr_elevation = max(0.0, latest_rhr - baseline_rhr)
    hemodynamic_strain = clamp((rhr_elevation / 6.0) * 100.0, 0.0, 100.0)

    sleep_debt_strain = clamp(100.0 - osas_score, 0.0, 100.0)
    stress_score = int(clamp(0.40 * autonomic_strain + 0.30 * hemodynamic_strain + 0.30 * sleep_debt_strain, 0.0, 100.0))

    if stress_score <= 25:
        stress_label = "Low"
    elif stress_score <= 50:
        stress_label = "Mild"
    elif stress_score <= 75:
        stress_label = "Moderate"
    else:
        stress_label = "High"

    return {
        "stress_score": stress_score,
        "autonomic_strain": round(autonomic_strain, 1),
        "hemodynamic_strain": round(hemodynamic_strain, 1),
        "sleep_debt_strain": round(sleep_debt_strain, 1),
        "stress_label": stress_label
    }

def calculate_fitness_age(curr_date, latest_rhr, baseline_rhr, avg_steps_7d, measured_vo2max=None):
    """
    Biological Fitness Age & Functional VO2 Max:
    1. Dynamic Chronological Age: date.year - 1997
    2. NTNU / CERG scale: age = 160.7 - 2.513 * VO2max
    3. ACSM / Lifestyle Functional Biological Age:
       Modulates the measured/cardiorespiratory baseline with weekly activity volume
       and resting heart rate recovery relative to personal baseline, compared to
       age-normative expected VO2 max.
    """
    chrono_age = float(curr_date.year - 1997)
    expected_vo2 = 44.0 - max(0.0, (chrono_age - 27.0) * 0.5)

    base_vo2 = measured_vo2max if (measured_vo2max is not None and measured_vo2max > 20.0) else 49.1

    # Physical activity modulation: 10,000 steps is +1.5 mL, 2,000 steps is -1.5 mL
    activity_factor = clamp(avg_steps_7d / 10000.0, 0.0, 1.0)
    activity_mod = (activity_factor - 0.5) * 3.0

    # Hemodynamic modulation: resting HR vs baseline
    b_rhr = baseline_rhr if (baseline_rhr is not None and baseline_rhr > 40.0) else 71.4
    rhr_diff = latest_rhr - b_rhr
    rhr_mod = clamp(rhr_diff / 4.0, -2.0, 2.0) * 1.5

    functional_vo2max = round(clamp(base_vo2 + activity_mod - rhr_mod, 25.0, 60.0), 1)

    # Biological Age: deviation from age-normative fitness (scaled by 1.25)
    vo2_gap = expected_vo2 - functional_vo2max
    bio_age = round(clamp(chrono_age + (vo2_gap * 1.25), 18.0, 60.0), 1)

    # NTNU / CERG Fitness Age (pure cardiopulmonary map)
    cerg_fitness_age = round(clamp(160.7 - (2.513 * base_vo2), 18.0, 70.0), 1)

    return {
        "chrono_age": chrono_age,
        "functional_vo2max": functional_vo2max,
        "bio_age": bio_age,
        "cerg_fitness_age": cerg_fitness_age,
        "effective_vo2max": base_vo2
    }

def calculate_autonomic_age(latest_hrv, baseline_hrv, latest_rhr, baseline_rhr, chrono_age):
    """
    Standard Autonomic / Vagal Biological Age (Umetani / Kubios Normative Model):
    Evaluates parasympathetic autonomic health via nocturnal ln(RMSSD) and resting heart rate.
    Expected ln(RMSSD) for healthy men declines linearly with age:
      ln(RMSSD_expected) = 4.25 - 0.018 * chrono_age
    Deviations reflect parasympathetic vitality (younger autonomic age) vs vagal withdrawal (older age).
    """
    expected_ln_rmssd = 4.25 - (0.018 * chrono_age)
    actual_ln_rmssd = math.log(max(5.0, latest_hrv))
    # Each 0.1 log unit deviation ~ 2.0 years of autonomic age difference
    hrv_delta_years = (expected_ln_rmssd - actual_ln_rmssd) * 20.0

    b_rhr = baseline_rhr if (baseline_rhr is not None and baseline_rhr > 40.0) else 71.4
    rhr_delta_years = clamp((latest_rhr - b_rhr) / 4.0, -2.0, 2.0) * 2.0

    autonomic_age = round(clamp(chrono_age + hrv_delta_years + rhr_delta_years, 18.0, 60.0), 1)
    return autonomic_age

def get_interpolated_vo2(curr_date, vo2_dict):
    """
    Interpolates baseline VO2Max across the 2-year measurement blindspot (Oct 2024 to Sep 2026)
    between the two actual outdoor GPS run tests (49.1 in Oct 2024 and 43.2 in Sep 2026).
    """
    d_oct_2024 = datetime.strptime("2024-10-10", "%Y-%m-%d").date()
    d_sep_2026 = datetime.strptime("2026-09-07", "%Y-%m-%d").date()

    if curr_date <= d_oct_2024:
        return 49.1
    elif curr_date >= d_sep_2026:
        curr_str = curr_date.strftime("%Y-%m-%d")
        matching = [d for d in vo2_dict if d <= curr_str]
        return vo2_dict[max(matching)] if matching else 43.2
    else:
        frac = (curr_date - d_oct_2024).days / (d_sep_2026 - d_oct_2024).days
        return round(49.1 + frac * (43.2 - 49.1), 2)

# =============================================================================
# Pipeline & Batch Processing
# =============================================================================

def parse_series_dict(series_data, date_col_idx=0, val_col_idx=1):
    """Helper to convert Influx series values to a date->value dict (date YYYY-MM-DD)."""
    res = {}
    if not series_data or "values" not in series_data:
        return res
    for row in series_data["values"]:
        raw_time = row[date_col_idx]
        if not raw_time:
            continue
        date_str = raw_time[:10]
        val = row[val_col_idx]
        if val is not None:
            res[date_str] = val
    return res

def fetch_telemetry_histories(since_date_str=None):
    """
    Pulls raw historical series from InfluxDB for processing.
    Returns:
      sleep_dict: date -> {minutesAsleep, minutesAwake, minutesDeep, minutesREM, minutesLight}
      hrv_dict: date -> float
      rhr_dict: date -> float
      steps_dict: date -> int
      vo2_dict: date -> float
    """
    time_filter = f" WHERE time >= '{since_date_str}T00:00:00Z'" if since_date_str else ""

    q = (
        f'SELECT minutesAsleep, minutesAwake, minutesDeep, minutesREM, minutesLight FROM "Sleep Summary"{time_filter}; '
        f'SELECT dailyRmssd FROM "HRV"{time_filter}; '
        f'SELECT value FROM "RestingHR"{time_filter}; '
        f'SELECT value FROM "Total Steps"{time_filter}; '
        f'SELECT value FROM "VO2Max"{time_filter}'
    )
    results = query_influx(q)
    if len(results) < 5:
        logger.error("Failed to query full telemetry history (received %d series).", len(results))
        return {}, {}, {}, {}, {}

    # 1. Sleep Summary
    sleep_dict = {}
    if "series" in results[0]:
        for row in results[0]["series"][0]["values"]:
            raw_time = row[0]
            if not raw_time:
                continue
            date_str = raw_time[:10]
            asleep = float(row[1]) if row[1] is not None else 0.0
            awake = float(row[2]) if row[2] is not None else 0.0
            deep = float(row[3]) if row[3] is not None else 0.0
            rem = float(row[4]) if row[4] is not None else 0.0
            light = float(row[5]) if row[5] is not None else 0.0

            # If multiple sessions on same date, keep the primary (longest) one
            if date_str not in sleep_dict or asleep > sleep_dict[date_str]["minutesAsleep"]:
                sleep_dict[date_str] = {
                    "minutesAsleep": asleep,
                    "minutesAwake": awake,
                    "minutesDeep": deep,
                    "minutesREM": rem,
                    "minutesLight": light
                }

    # 2. HRV
    hrv_dict = {}
    if "series" in results[1]:
        for row in results[1]["series"][0]["values"]:
            if row[0] and row[1] is not None:
                hrv_dict[row[0][:10]] = float(row[1])

    # 3. Resting HR
    rhr_dict = {}
    if "series" in results[2]:
        for row in results[2]["series"][0]["values"]:
            if row[0] and row[1] is not None:
                rhr_dict[row[0][:10]] = float(row[1])

    # 4. Steps
    steps_dict = {}
    if "series" in results[3]:
        for row in results[3]["series"][0]["values"]:
            if row[0] and row[1] is not None:
                steps_dict[row[0][:10]] = float(row[1])

    # 5. VO2 Max
    vo2_dict = {}
    if "series" in results[4]:
        for row in results[4]["series"][0]["values"]:
            if row[0] and row[1] is not None:
                vo2_dict[row[0][:10]] = float(row[1])

    return sleep_dict, hrv_dict, rhr_dict, steps_dict, vo2_dict

def compute_rolling_stats(history_dict, target_date, days=30):
    """
    Computes mean and sample stddev for the specified number of days prior to target_date.
    target_date: datetime.date
    """
    vals = []
    for d in range(1, days + 1):
        prev_date = (target_date - timedelta(days=d)).strftime("%Y-%m-%d")
        if prev_date in history_dict:
            vals.append(history_dict[prev_date])
    if not vals:
        return None, None
    mean_val = sum(vals) / len(vals)
    if len(vals) > 1:
        variance = sum((x - mean_val) ** 2 for x in vals) / (len(vals) - 1)
        std_val = math.sqrt(variance)
    else:
        std_val = 0.0
    return mean_val, std_val

def compute_scores_for_timeline(sleep_dict, hrv_dict, rhr_dict, steps_dict, vo2_dict, target_dates):
    """
    Iterates over target_dates, computes daily scores, and generates InfluxDB line protocol records.
    """
    points = []
    last_known_vo2 = 49.1  # Earliest recorded watch baseline (Aug 2024)

    # Sort all dates to track chronological VO2Max updates
    all_dates_sorted = sorted(set(list(sleep_dict.keys()) + list(hrv_dict.keys()) + list(rhr_dict.keys())))

    for date_str in target_dates:
        try:
            curr_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            continue

        # Interpolate baseline VO2Max across the 2-year run gap
        base_vo2 = get_interpolated_vo2(curr_date, vo2_dict)

        # 1. Sleep Score (OSAS)
        sleep_entry = sleep_dict.get(date_str)
        if sleep_entry and sleep_entry["minutesAsleep"] > 0:
            osas_res = calculate_osas(
                sleep_entry["minutesAsleep"],
                sleep_entry["minutesDeep"],
                sleep_entry["minutesREM"],
                sleep_entry["minutesLight"],
                sleep_entry["minutesAwake"]
            )
            osas_score = osas_res["osas_score"]
            restorative_ratio = osas_res["restorative_ratio"]
            total_sleep_hours = osas_res["total_sleep_hours"]
        else:
            osas_score = 70  # neutral fallback
            restorative_ratio = 33.0
            total_sleep_hours = 7.0

        # 2. HRV Telemetry & 30-day baseline
        curr_hrv = hrv_dict.get(date_str)
        mean_hrv, std_hrv = compute_rolling_stats(hrv_dict, curr_date, days=30)
        baseline_hrv = mean_hrv if mean_hrv is not None else (curr_hrv if curr_hrv is not None else 42.6)
        hrv_std = std_hrv if (std_hrv is not None and std_hrv > 0) else 8.0
        latest_hrv = curr_hrv if curr_hrv is not None else baseline_hrv

        # 3. Resting HR Telemetry & 30-day baseline
        curr_rhr = rhr_dict.get(date_str)
        mean_rhr, std_rhr = compute_rolling_stats(rhr_dict, curr_date, days=30)
        baseline_rhr = mean_rhr if mean_rhr is not None else (curr_rhr if curr_hrv is not None else 71.4)
        latest_rhr = curr_rhr if curr_rhr is not None else baseline_rhr

        # 4. 7-Day Steps
        steps_vals = []
        for d in range(1, 8):
            p_date = (curr_date - timedelta(days=d)).strftime("%Y-%m-%d")
            if p_date in steps_dict:
                steps_vals.append(steps_dict[p_date])
        avg_steps_7d = sum(steps_vals) / len(steps_vals) if steps_vals else 4000.0

        # Calculations
        readiness_res = calculate_readiness(latest_hrv, baseline_hrv, hrv_std, latest_rhr, baseline_rhr, osas_score)
        stress_res = calculate_stress(latest_hrv, baseline_hrv, latest_rhr, baseline_rhr, osas_score)
        fitness_res = calculate_fitness_age(curr_date, latest_rhr, baseline_rhr, avg_steps_7d, base_vo2)
        autonomic_age = calculate_autonomic_age(latest_hrv, baseline_hrv, latest_rhr, baseline_rhr, fitness_res['chrono_age'])

        # Skip writing if no actual biometric data exists at all for this day
        if date_str not in sleep_dict and date_str not in hrv_dict and date_str not in rhr_dict:
            continue

        # Format InfluxDB Line Protocol
        # Timestamp: Midnight UTC on date_str in nanoseconds
        timestamp_ns = int(datetime(curr_date.year, curr_date.month, curr_date.day, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1e9)

        fields = [
            f"readiness_score={readiness_res['readiness_score']}",
            f"sleep_score={osas_score}",
            f"stress_score={stress_res['stress_score']}",
            f"bio_age={fitness_res['bio_age']}",
            f"cerg_fitness_age={fitness_res['cerg_fitness_age']}",
            f"autonomic_age={autonomic_age}",
            f"target_age=22.0",
            f"chrono_age={fitness_res['chrono_age']}",
            f"functional_vo2max={fitness_res['functional_vo2max']}",
            f"effective_vo2max={fitness_res['effective_vo2max']}",
            f"hrv_score={readiness_res['hrv_score']}",
            f"rhr_score={readiness_res['rhr_score']}",
            f"autonomic_strain={stress_res['autonomic_strain']}",
            f"hemodynamic_strain={stress_res['hemodynamic_strain']}",
            f"sleep_debt_strain={stress_res['sleep_debt_strain']}",
            f"restorative_ratio={restorative_ratio}",
            f"total_sleep_hours={total_sleep_hours}",
            f"baseline_hrv={round(baseline_hrv, 2)}",
            f"baseline_rhr={round(baseline_rhr, 2)}"
        ]
        field_str = ",".join(fields)
        line = f"Daily_Scores,user=prasad,source=calculated_engine {field_str} {timestamp_ns}"
        points.append(line)

    return points

def backfill_all():
    """Backfills the entire historical timeline from 2024-05-17 to today."""
    logger.info("Starting complete historical biometric score backfill...")
    sleep_dict, hrv_dict, rhr_dict, steps_dict, vo2_dict = fetch_telemetry_histories()

    all_dates = sorted(set(list(sleep_dict.keys()) + list(hrv_dict.keys()) + list(rhr_dict.keys()) + list(steps_dict.keys())))
    if not all_dates:
        logger.error("No historical telemetry found in InfluxDB to backfill.")
        return False

    logger.info("Found %d distinct calendar days of telemetry across %s to %s",
                len(all_dates), all_dates[0], all_dates[-1])

    lines = compute_scores_for_timeline(sleep_dict, hrv_dict, rhr_dict, steps_dict, vo2_dict, all_dates)
    logger.info("Generated %d daily score records. Writing in batches...", len(lines))

    # Write in batches of 200 lines
    batch_size = 200
    for i in range(0, len(lines), batch_size):
        batch = lines[i:i + batch_size]
        success = write_influx_lines(batch)
        if not success:
            logger.error("Failed writing batch %d-%d", i, i + len(batch))
            return False

    logger.info("Successfully backfilled %d daily score records into 'Daily_Scores' measurement!", len(lines))
    return True

def sync_recent(days=3):
    """Calculates and updates scores for the most recent N days."""
    logger.info("Synchronizing recent daily scores (lookback: %d days)...", days)
    # Pull last 45 days to maintain accurate 30-day rolling baseline
    cutoff_date = (datetime.now(timezone.utc) - timedelta(days=days + 45)).strftime("%Y-%m-%d")
    sleep_dict, hrv_dict, rhr_dict, steps_dict, vo2_dict = fetch_telemetry_histories(cutoff_date)

    target_dates = [
        (datetime.now(timezone.utc) - timedelta(days=d)).strftime("%Y-%m-%d")
        for d in range(days)
    ]

    lines = compute_scores_for_timeline(sleep_dict, hrv_dict, rhr_dict, steps_dict, vo2_dict, target_dates)
    if lines:
        success = write_influx_lines(lines)
        if success:
            logger.info("Successfully synced %d recent daily score points into 'Daily_Scores'.", len(lines))
            return True
        else:
            logger.error("Failed to write recent daily score points.")
            return False
    else:
        logger.info("No score lines generated for recent dates %s", target_dates)
        return True

if __name__ == "__main__":
    if "--backfill" in sys.argv:
        backfill_all()
    elif "--recent" in sys.argv:
        lookback = 3
        if len(sys.argv) > 2 and sys.argv[2].isdigit():
            lookback = int(sys.argv[2])
        sync_recent(lookback)
    else:
        # Default run: sync recent
        sync_recent(3)
