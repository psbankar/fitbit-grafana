#!/bin/bash
set -e

# Default to webhook mode if ENABLE_WEBHOOK_SERVER is set or true
if [ "${ENABLE_WEBHOOK_SERVER}" = "true" ] || [ "${ENABLE_WEBHOOK_SERVER}" = "True" ] || [ "${ENABLE_WEBHOOK_SERVER}" = "1" ]; then
    echo "Starting Google Health Event-Driven Webhook Server on port ${WEBHOOK_PORT:-8000}..."
    exec python webhook_server.py
else
    echo "Starting standard Fitbit_Fetch data poller..."
    exec python Fitbit_Fetch.py
fi
