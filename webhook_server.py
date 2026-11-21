#!/usr/bin/env python3
"""
FastAPI Event-Driven Webhook Server for Google Health API & InfluxDB.
Receives real-time data change notifications via Google DataSubscriptionService,
handles registration challenge handshakes, coalesces rapid notification bursts,
and writes structured time-series metrics to InfluxDB without data voids.
"""

import os
import sys
import json
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
from fastapi import FastAPI, Request, Response, BackgroundTasks, Header
import uvicorn
import pytz

# Ingestion functions from Fitbit_Fetch
import Fitbit_Fetch as ff

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] [WebhookServer] - %(message)s"
)

WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")
LISTEN_PORT = int(os.environ.get("WEBHOOK_PORT", "8000"))
LISTEN_HOST = os.environ.get("WEBHOOK_HOST", "0.0.0.0")

# Coalescing queue
notification_queue: asyncio.Queue = asyncio.Queue()
worker_task: Optional[asyncio.Task] = None
safety_sweep_task: Optional[asyncio.Task] = None


async def safety_sweep_worker():
    """
    Periodic safety reconciliation sweep running every 24 hours to catch
    any edge-case device syncs where a webhook delivery might have blipped.
    """
    logging.info("Periodic 24-hour safety reconciliation worker started.")
    while True:
        try:
            # Run every 24 hours (86400 seconds)
            await asyncio.sleep(86400)
            logging.info("Executing scheduled 24-hour safety reconciliation sweep...")
            await notification_queue.put({"dataType": "SAFETY_SWEEP", "date": datetime.now(ff.LOCAL_TIMEZONE).strftime("%Y-%m-%d")})
        except asyncio.CancelledError:
            break
        except Exception as e:
            logging.error("Error in safety sweep worker: %s", e)


async def coalescing_worker():
    """
    Background worker that drains the notification queue and batches incoming
    signals over a 2.5s window to prevent redundant concurrent queries.
    """
    logging.info("Coalescing webhook ingest worker started.")
    while True:
        try:
            # Wait for at least one notification
            item = await notification_queue.get()
            batch = [item]
            notification_queue.task_done()

            # Wait briefly to collect any companion notifications from the same sync burst
            await asyncio.sleep(2.5)

            while not notification_queue.empty():
                try:
                    extra = notification_queue.get_nowait()
                    batch.append(extra)
                    notification_queue.task_done()
                except asyncio.QueueEmpty:
                    break

            logging.info("Processing coalesced notification batch with %d event(s)", len(batch))
            process_coalesced_batch(batch)

        except asyncio.CancelledError:
            logging.info("Coalescing worker received cancellation.")
            break
        except Exception as e:
            logging.error("Unexpected error in coalescing worker: %s", e)


def process_coalesced_batch(batch: List[Dict[str, Any]]):
    """
    Executes targeted data retrieval based on the data types and dates reported
    in the webhook batch. Guarantees full interval capture with no data voids.
    """
    today_str = datetime.now(ff.LOCAL_TIMEZONE).strftime("%Y-%m-%d")
    yesterday_str = (datetime.now(ff.LOCAL_TIMEZONE) - timedelta(days=1)).strftime("%Y-%m-%d")

    data_types_seen = set()
    dates_seen = set()

    for event in batch:
        dt = event.get("dataType", "").upper()
        if dt:
            data_types_seen.add(dt)
        if "date" in event:
            dates_seen.add(event["date"])
        # Also extract dates from intervals if present (e.g. Google physicalTimeInterval)
        intervals = event.get("intervals") or []
        for interval in intervals:
            if isinstance(interval, dict):
                p_interval = interval.get("physicalTimeInterval") or interval
                start_time = p_interval.get("startTime")
                if start_time:
                    try:
                        dt_obj = datetime.fromisoformat(start_time.replace("Z", "+00:00")).astimezone(ff.LOCAL_TIMEZONE)
                        dates_seen.add(dt_obj.strftime("%Y-%m-%d"))
                    except Exception:
                        pass

    if not dates_seen:
        dates_seen.add(today_str)
        dates_seen.add(yesterday_str)

    is_all = any(x in data_types_seen for x in ["ALL", "STARTUP_SYNC", "SAFETY_SWEEP"]) or not data_types_seen

    logging.info("Executing targeted ingest for data types: %s (is_all=%s) across dates: %s",
                 list(data_types_seen), is_all, list(dates_seen))

    try:
        # Refresh token before querying
        if ff.HEALTH_API_PROVIDER == "google":
            ff.Get_New_Access_Token(ff.client_id, ff.client_secret)

        # 1. Heart rate & steps intraday
        if is_all or any(t in dt for dt in data_types_seen for t in ["HEART", "STEP"]):
            for date_str in sorted(dates_seen):
                ff.get_intraday_data_limit_1d(date_str, [
                    ('heart', 'HeartRate_Intraday', '1sec'),
                    ('steps', 'Steps_Intraday', '1min')
                ])

        # 2. Sleep sessions and stages
        if is_all or any("SLEEP" in dt for dt in data_types_seen):
            ff.get_daily_data_limit_100d(yesterday_str, today_str)

        # 3. Workouts and TCX GPS
        if is_all or any(t in dt for dt in data_types_seen for t in ["EXERCISE", "ACTIVITY"]):
            ff.fetch_latest_activities(today_str)

        # 4. Daily vitals, clinical metrics, nutrition & wellness logs
        vital_keywords = [
            "HRV", "RESPIRATORY", "TEMPERATURE", "SPO2", "WEIGHT", "BODY_FAT", "VO2",
            "GLUCOSE", "PRESSURE", "MINDFUL", "MOOD", "SYMPTOM", "ECG", "HYDRATION", "NUTRITION", "SWIM"
        ]
        if is_all or any(t in dt for dt in data_types_seen for t in vital_keywords):
            ff.get_daily_data_limit_30d(yesterday_str, today_str)
            ff.get_daily_data_limit_none(yesterday_str, today_str)
            ff.get_daily_data_limit_365d(yesterday_str, today_str)

        # 5. Battery and paired device info
        ff.get_battery_level()

        # Write to InfluxDB
        record_count = len(ff.collected_records)
        if record_count > 0:
            ff.write_points_to_influxdb(ff.collected_records)
            logging.info("Successfully ingested %d points from webhook event batch", record_count)
            ff.collected_records.clear()
        else:
            logging.info("Webhook event batch processed; no new points to write.")

    except Exception as e:
        logging.error("Failed to process webhook ingest batch: %s", e)


from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    global worker_task, safety_sweep_task
    worker_task = asyncio.create_task(coalescing_worker())
    safety_sweep_task = asyncio.create_task(safety_sweep_worker())
    logging.info("FastAPI Webhook Server initialized on %s:%d", LISTEN_HOST, LISTEN_PORT)
    # Enqueue immediate startup sync to fill any gap since last run
    await notification_queue.put({"dataType": "STARTUP_SYNC", "received_at": datetime.now(timezone.utc).isoformat()})
    yield
    if worker_task:
        worker_task.cancel()
    if safety_sweep_task:
        safety_sweep_task.cancel()
    try:
        if worker_task:
            await worker_task
        if safety_sweep_task:
            await safety_sweep_task
    except asyncio.CancelledError:
        pass
    logging.info("FastAPI Webhook Server stopped.")


app = FastAPI(
    title="Google Health Ingestion Webhook Service",
    description="Real-time event receiver for Google Health API DataSubscriptionService",
    version="1.0.0",
    lifespan=lifespan
)


@app.get("/healthz")
async def healthz():
    """Health check endpoint for SWAG reverse proxy and container orchestrator."""
    return {
        "status": "ok",
        "provider": ff.HEALTH_API_PROVIDER,
        "queue_pending": notification_queue.qsize(),
        "utc_time": datetime.now(timezone.utc).isoformat()
    }


@app.post("/webhook")
async def receive_webhook(request: Request, response: Response):
    """
    Main webhook receiver for Google Health API DataSubscriptionService.
    Handles registration challenge handshakes and enqueues event payloads.
    """
    auth_header = request.headers.get("Authorization", "")

    # Security Validation: Google verifies that requests without the secret fail (401),
    # and requests with the secret pass (200/201).
    if WEBHOOK_SECRET:
        expected = f"Bearer {WEBHOOK_SECRET}"
        if auth_header != expected:
            logging.warning("Rejected webhook probe: missing or invalid Authorization header: '%s'", auth_header)
            return Response(status_code=401, content="Unauthorized")

    try:
        raw_body = await request.body()
        body = json.loads(raw_body.decode('utf-8')) if raw_body else {}
    except Exception:
        body = {}

    logging.info("Received authenticated webhook notification from Google Health API")

    items = []
    if isinstance(body, list):
        items = body
    elif isinstance(body, dict):
        if "notifications" in body and isinstance(body["notifications"], list):
            items = body["notifications"]
        elif "events" in body and isinstance(body["events"], list):
            items = body["events"]
        elif "data" in body or "dataType" in body:
            items = [body]

    if items:
        for item in items:
            event = item.get("data") if (isinstance(item, dict) and "data" in item) else item
            if isinstance(event, dict):
                await notification_queue.put(event)
    else:
        # Registration test probe or generic ping -> trigger full sync
        await notification_queue.put({"dataType": "ALL", "received_at": datetime.now(timezone.utc).isoformat()})

    # Return 201 Created as required by Google Health API subscriber verification handshake
    return Response(status_code=201, content=json.dumps({"status": "accepted", "timestamp": datetime.now(timezone.utc).isoformat()}), media_type="application/json")


if __name__ == "__main__":
    uvicorn.run("webhook_server:app", host=LISTEN_HOST, port=LISTEN_PORT, log_level="info")
