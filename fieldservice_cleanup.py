"""Schedule and run a photo-retention sweep for field-service work orders."""
import json
import os
from datetime import date, timedelta

import infrai


def stale_photo_ids(records, today, retention_days=30):
    cutoff = today - timedelta(days=retention_days)
    return [record["photo_id"] for record in records if record["dispatch_status"] == "closed" and record["technician_follow_up"] == "complete" and date.fromisoformat(record["photo_taken_on"]) < cutoff]


def schedule_sweep():
    result = infrai.cron.create(cron_expr="0 2 * * *", task=os.environ["FIELD_SERVICE_CLEANUP_URL"])
    return result["job_id"]


def publish_cleanup(photo_ids):
    payload = json.dumps({"action": "delete_work_order_photos", "photo_ids": photo_ids})
    return infrai.queue.publish(queue="work-order-photo-cleanup", payload=payload)


def run_sweep(records, today=None):
    today = today or date.today()
    photo_ids = stale_photo_ids(records, today)
    if not photo_ids:
        return {"deleted_photo_ids": [], "published": False}
    publish_cleanup(photo_ids)
    return {"deleted_photo_ids": photo_ids, "published": True}


if __name__ == "__main__":
    job_id = schedule_sweep()
    print(f"scheduled cleanup job: {job_id}")
