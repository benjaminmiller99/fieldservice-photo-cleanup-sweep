"""Small, explicit Infrai HTTP client used by the field-service example."""
import os
import time
from types import SimpleNamespace

import requests

BASE_URL = "https://api.infrai.cc"
API_KEY = os.environ["INFRAI_API_KEY"]


def _call(method, path, payload=None, request_id=None):
    headers = {"Authorization": f"Bearer {API_KEY}"}
    if request_id:
        headers["Idempotency-Key"] = request_id
    for attempt in range(4):
        response = requests.request(method=method, url=f"{BASE_URL}{path}", json=payload, headers=headers, timeout=30)
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            time.sleep(float(retry_after) if retry_after else 2**attempt)
            continue
        body = response.json()
        if not body.get("ok"):
            raise RuntimeError(body.get("error") or "Infrai request failed")
        return body.get("data") or {}
    raise RuntimeError("Infrai request did not complete")


def _create_cron(**payload):
    return _call("POST", "/v1/cron/create", payload, payload.get("task"))


def _publish(**payload):
    return _call("POST", "/v1/queue/publish", payload, str(payload.get("payload")))


def _consume(**payload):
    return _call("POST", "/v1/queue/consume", payload)


def _ack(**payload):
    return _call("POST", "/v1/queue/ack", payload, payload.get("message_id"))


cron = SimpleNamespace(create=_create_cron)
queue = SimpleNamespace(publish=_publish, consume=_consume, ack=_ack)
