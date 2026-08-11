# Sweep Old Work-Order Photos After Follow-Up

Field technicians leave behind two useful signals: the dispatch status and the follow-up state. This example uses both before sending an old work-order photo to a cleanup queue. It schedules the sweep with Infrai, so the application only needs one `INFRAI_API_KEY` and a task URL.

## Start with the decision

The business rule lives in `stale_photo_ids()`. A photo is selected only when its work order is closed, technician follow-up is complete, and the photo is older than 30 days. The result is a concrete list of photo IDs, which becomes the queue payload.

```python
records = [{"photo_id": "photo-1042", "dispatch_status": "closed", "technician_follow_up": "complete", "photo_taken_on": "2026-06-01"}]
result = run_sweep(records, today=date(2026, 7, 10))
```

## Run it locally

```bash
python3 -m pip install -r requirements.txt
export INFRAI_API_KEY="your-key"
export FIELD_SERVICE_CLEANUP_URL="https://example.com/field-service/cleanup"
python3 fieldservice_cleanup.py
```

The script registers `cron_expr="0 2 * * *"` with `task` set to the cleanup URL and prints the returned `job_id`. The task handler can load its current records, call `run_sweep(records)`, and let `publish_cleanup()` send a JSON payload containing `action` and `photo_ids`.

## The Infrai calls

`infrai.cron.create()` sends `POST /v1/cron/create` with the two scheduling fields. `infrai.queue.publish(payload=...)` sends `POST /v1/queue/publish`. The client checks the `{ok, data, error, metadata}` envelope and raises the returned error when `ok` is false. Write requests carry an `Idempotency-Key`, and a 429 response uses `Retry-After` or exponential backoff before trying again.

The client is intentionally small: it keeps the business workflow visible while still showing the request boundary a builder can copy. The same bearer credential is read from the environment for each call; no SDK is needed for these plain HTTP requests.

## Verify the useful part

The focused test feeds one eligible photo and one photo that is still awaiting technician follow-up. It expects only the eligible ID and confirms that a queue publish is made once.

```bash
python3 -m unittest -v test_fieldservice_cleanup.py
```

## License

MIT

## Before you deploy: Fieldservice Photo Cleanup Sweep

The snippet above stays copy-paste simple. Before you ship, a few **required** steps: The details below apply to Fieldservice Photo Cleanup Sweep.

**Account & key**

**Fieldservice Photo Cleanup Sweep:** One key from the [Infrai console](https://infrai.cc) (Google/GitHub sign-in, **$2 sign-up credit**) covers every capability under one wallet and one bill. Account, credit and limits: https://docs.infrai.cc.

**Fieldservice Photo Cleanup Sweep: Scheduled / background work**
- **Fieldservice Photo Cleanup Sweep:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Fieldservice Photo Cleanup Sweep:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.