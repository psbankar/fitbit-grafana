#!/usr/bin/env python3
"""
CLI Tool to manage Google Health API Webhook Subscriptions (DataSubscriptionService).
Usage:
    python manage_subscriptions.py register --secret <secret> [--url https://health.772723.xyz/webhook]
    python manage_subscriptions.py list
    python manage_subscriptions.py delete [--id boreas-fitbit-subscriber]
"""

import os
import sys
import json
import logging
import argparse
import requests
import Fitbit_Fetch as ff

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] %(message)s")

DEFAULT_SUBSCRIBER_ID = "boreas-fitbit-subscriber"
DEFAULT_PUBLIC_URL = os.environ.get("WEBHOOK_PUBLIC_URL", "https://health.772723.xyz/webhook")


def get_project_id():
    # Attempt to extract project id from env or GOOGLE_CLIENT_ID
    project_id = os.environ.get("GCP_PROJECT_ID")
    if not project_id and ff.client_id:
        # e.g., 693456048191-g0njbukbi31dhjr4b6u3bg3s8t66360l.apps.googleusercontent.com -> 693456048191
        client_parts = ff.client_id.split("-")
        if client_parts and client_parts[0].isdigit():
            project_id = client_parts[0]
    return project_id or "693456048191"


def get_auth_headers():
    ff.Get_New_Access_Token(ff.client_id, ff.client_secret)
    return {
        "Authorization": f"Bearer {ff.ACCESS_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }


def register_subscriber(subscriber_id, endpoint_url, webhook_secret):
    project_id = get_project_id()
    headers = get_auth_headers()

    url = f"{ff.GOOGLE_HEALTH_BASE_URL}/{ff.GOOGLE_HEALTH_API_VERSION}/projects/{project_id}/subscribers?subscriberId={subscriber_id}"
    payload = {
        "endpointUri": endpoint_url,
        "endpointAuthorization": {
            "secret": f"Bearer {webhook_secret}"
        }
    }

    logging.info("Registering subscriber '%s' with endpoint: %s", subscriber_id, endpoint_url)
    resp = requests.post(url, headers=headers, json=payload)
    if resp.status_code in [200, 201]:
        logging.info("Successfully registered subscriber '%s': %s", subscriber_id, resp.json())
    elif resp.status_code == 409:
        logging.info("Subscriber '%s' already exists; updating configuration...", subscriber_id)
        update_url = f"{ff.GOOGLE_HEALTH_BASE_URL}/{ff.GOOGLE_HEALTH_API_VERSION}/projects/{project_id}/subscribers/{subscriber_id}"
        patch_resp = requests.patch(update_url, headers=headers, json=payload)
        logging.info("Update result (%d): %s", patch_resp.status_code, patch_resp.text)
    else:
        logging.error("Failed to register subscriber (%d): %s", resp.status_code, resp.text)
        return False

    # Bind user subscriptions to subscriber
    sub_url = f"{ff.GOOGLE_HEALTH_BASE_URL}/{ff.GOOGLE_HEALTH_API_VERSION}/projects/{project_id}/subscribers/{subscriber_id}/subscriptions"
    sub_resp = requests.post(sub_url, headers=headers, json={"user": "users/me"})
    if sub_resp.status_code in [200, 201]:
        logging.info("Successfully bound user subscription to subscriber '%s'", subscriber_id)
    else:
        logging.warning("User subscription bind response (%d): %s", sub_resp.status_code, sub_resp.text)
    return True


def list_subscribers():
    project_id = get_project_id()
    headers = get_auth_headers()

    url = f"{ff.GOOGLE_HEALTH_BASE_URL}/{ff.GOOGLE_HEALTH_API_VERSION}/projects/{project_id}/subscribers"
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        data = resp.json()
        logging.info("Active Subscribers for project %s:\n%s", project_id, json.dumps(data, indent=2))
    else:
        logging.error("Failed to list subscribers (%d): %s", resp.status_code, resp.text)


def delete_subscriber(subscriber_id):
    project_id = get_project_id()
    headers = get_auth_headers()

    url = f"{ff.GOOGLE_HEALTH_BASE_URL}/{ff.GOOGLE_HEALTH_API_VERSION}/projects/{project_id}/subscribers/{subscriber_id}"
    resp = requests.delete(url, headers=headers)
    if resp.status_code in [200, 204]:
        logging.info("Successfully deleted subscriber '%s'", subscriber_id)
    else:
        logging.error("Failed to delete subscriber (%d): %s", resp.status_code, resp.text)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Google Health API Webhook Subscription Manager")
    subparsers = parser.add_subparsers(dest="action", required=True)

    reg_p = subparsers.add_parser("register", help="Register a new subscriber endpoint")
    reg_p.add_argument("--id", default=DEFAULT_SUBSCRIBER_ID, help="Subscriber ID")
    reg_p.add_argument("--url", default=DEFAULT_PUBLIC_URL, help="Public HTTPS webhook URL")
    reg_p.add_argument("--secret", default=os.environ.get("WEBHOOK_SECRET", ""), help="Webhook secret authorization token")

    subparsers.add_parser("list", help="List active subscribers")

    del_p = subparsers.add_parser("delete", help="Delete a subscriber")
    del_p.add_argument("--id", default=DEFAULT_SUBSCRIBER_ID, help="Subscriber ID to delete")

    args = parser.parse_args()

    if args.action == "register":
        if not args.secret:
            logging.error("A webhook secret must be provided via --secret or WEBHOOK_SECRET env var")
            sys.exit(1)
        register_subscriber(args.id, args.url, args.secret)
    elif args.action == "list":
        list_subscribers()
    elif args.action == "delete":
        delete_subscriber(args.id)
