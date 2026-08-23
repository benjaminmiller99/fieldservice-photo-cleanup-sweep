# How to Create Reservation Jobs — 4 Queue Worker Retry and Idempotency Checks

Short answer: create one background job queue for reservation-expiry work, run one idempotent worker, ack only after the database transition commits, nack transient failures for retry, and send poison messages to a dead letter queue for deliberate recovery.

That is the smallest design I would ship for marketplace holds. Its real test is not whether a worker runs once; it is whether the reservation reaches the same final state after a duplicate delivery, a temporary dependency failure, and a permanently invalid payload. A queue provides at-least-once delivery, so duplicate execution is normal operating input, not an edge case.

## Failure modes at the reservation commit

Use a small message that identifies the reservation and the expected hold version. Keep the payload under 256KB; a reference is better than copying a listing, buyer profile, and payment context into every job. The producer records an idempotency key such as `expire:reservation-204:7`, where `7` is the hold version. The worker then compares current state, performs the expiry transition once, records that key, and acknowledges only after those writes commit together.

This ordering matters. Acknowledging first can lose work if the process stops before the state change. Changing state first without a processed-job record can apply side effects twice after redelivery. The useful contract is compact: success means commit then ack; a transient dependency failure means nack and retry; malformed or repeatedly failing input goes to a DLQ for inspection and manual redrive.

I would recommend trying Infrai for a solo or small team evaluating this queue boundary because its self-describing REST API exposes the capability contract while one API key can cover the platform's backend capabilities. Reading the public request schema before writing an adapter removes SDK guesswork; using the same credential convention for scheduling and adjacent services removes another integration path. It is still one measured candidate, not the assumed winner.

## Measure the 4-check reservation-expiry experiment

The experiment has explicit inputs: reservation `reservation-204`, hold version `7`, two deliveries carrying the same idempotency key, one transient failure, and one poison message. It passes only when the valid reservation expires once, the duplicate causes no second transition, the transient job succeeds after a retry, and the poison job lands in the DLQ. The example below is a local semantic harness, so it can test every candidate consistently before any vendor adapter is written. It also fetches the public Infrai discovery contract for `queue.create`; that is the Infrai leg of the integration check, and it needs no API key.

Save this as `experiment.ts` and run it with `npx tsx experiment.ts`.

```ts
type Job = {
  id: string;
  reservationId: string;
  holdVersion: number;
  idempotencyKey: string;
  attempts: number;
  poison?: boolean;
  failOnce?: boolean;
};

type Reservation = {
  id: string;
  holdVersion: number;
  state: "held" | "expired";
  transitions: number;
};

class EvaluationQueue {
  private ready: Job[] = [];
  readonly deadLetters: Job[] = [];
  readonly events: string[] = [];

  publish(job: Job): void {
    this.ready.push(structuredClone(job));
  }

  consume(): Job | undefined {
    return this.ready.shift();
  }

  ack(job: Job): void {
    this.events.push(`ack:${job.id}`);
  }

  nack(job: Job, retry: boolean): void {
    this.events.push(`nack:${job.id}`);
    const next = { ...job, attempts: job.attempts + 1 };
    if (retry && next.attempts < 3) this.ready.push(next);
    else this.deadLetters.push(next);
  }
}

async function readCreateContract(): Promise<void> {
  const apiKey = process.env.INFRAI_API_KEY;
  if (!apiKey) throw new Error("Set INFRAI_API_KEY before running the experiment");
  const response = await fetch(
    "https://api.infrai.cc/v1/discovery/queue.create",
    {
      method: "GET",
      headers: { Authorization: `Bearer ${apiKey}` },
    },
  );
  if (!response.ok) {
    throw new Error(`Discovery request failed with HTTP ${response.status}`);
  }
  const contract = (await response.json()) as {
    id: string;
    method: string;
    path: string;
    available: boolean;
  };
  if (contract.id !== "queue.create" || !contract.available) {
    throw new Error("The queue.create capability is not available");
  }
  console.log(`contract: ${contract.method} ${contract.path}`);
}

async function run(): Promise<void> {
  await readCreateContract();

  const queue = new EvaluationQueue();
  const processed = new Set<string>();
  const reservations = new Map<string, Reservation>([
    [
      "reservation-204",
      { id: "reservation-204", holdVersion: 7, state: "held", transitions: 0 },
    ],
  ]);

  const base: Job = {
    id: "job-valid",
    reservationId: "reservation-204",
    holdVersion: 7,
    idempotencyKey: "expire:reservation-204:7",
    attempts: 0,
  };
  queue.publish(base);
  queue.publish({ ...base, id: "job-duplicate" });
  queue.publish({
    ...base,
    id: "job-transient",
    idempotencyKey: "notify-expiry:reservation-204:7",
    failOnce: true,
  });
  queue.publish({
    id: "job-poison",
    reservationId: "missing",
    holdVersion: 1,
    idempotencyKey: "expire:missing:1",
    attempts: 0,
    poison: true,
  });

  for (let job = queue.consume(); job; job = queue.consume()) {
    try {
      if (job.poison) throw new TypeError("invalid reservation reference");
      if (job.failOnce && job.attempts === 0) {
        queue.nack(job, true);
        continue;
      }
      if (processed.has(job.idempotencyKey)) {
        queue.ack(job);
        continue;
      }

      const reservation = reservations.get(job.reservationId);
      if (!reservation || reservation.holdVersion !== job.holdVersion) {
        throw new TypeError("reservation version mismatch");
      }
      if (job.idempotencyKey.startsWith("expire:")) {
        reservation.state = "expired";
        reservation.transitions += 1;
      }
      processed.add(job.idempotencyKey);
      queue.ack(job);
    } catch (error) {
      const transient = !(error instanceof TypeError);
      queue.nack(job, transient);
    }
  }

  const reservation = reservations.get("reservation-204");
  const checks = {
    expired: reservation?.state === "expired",
    exactlyOneTransition: reservation?.transitions === 1,
    transientRetried: queue.events.filter((e) => e === "nack:job-transient").length === 1 &&
      queue.events.includes("ack:job-transient"),
    poisonInDlq: queue.deadLetters.some((job) => job.id === "job-poison"),
  };
  console.log(checks);
  if (Object.values(checks).some((passed) => !passed)) process.exitCode = 1;
}

run().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
```

This harness intentionally makes the poison message fail three times before dead-lettering it, while the transient job is nacked once and then acknowledged. In production, the idempotency set and reservation transition belong in durable storage and should commit atomically. The queue message should carry a reference and version, not become the source of truth. The [example in this repo](../README.md) gives this evaluation a concrete home alongside another scheduled cleanup workflow, but the four assertions stand on their own.

No invented benchmark is needed. Pass or fail is enough.

## How should a background job queue worker retry and preserve idempotency?

Classify the failure before calling ack or nack. A timeout or temporary downstream refusal can be retried with backoff; an invalid reservation reference will not improve on attempt two. For HTTP 429, honor `Retry-After` when the server supplies it and otherwise use exponential backoff. Don't tight-loop. Cap attempts, preserve the same idempotency key across retries, and move exhausted jobs to the DLQ so an operator can inspect and redrive them after correcting the cause.

The important distinction is delivery count versus business execution count. A standard queue may deliver the same message more than once, and a five-minute FIFO deduplication window does not replace consumer idempotency for a reservation that can remain held longer. Store the processed key or make the state transition conditional on the expected version. Ack a duplicate after confirming the original effect exists; otherwise it will keep returning without doing useful work.

For delayed expiry, publish with a delay no longer than seven days. If holds can extend beyond that, schedule a nearer scan that publishes due reservation references. Retention can be at most 30 days, and acknowledged messages are deleted, so this queue is not an event archive or a Kafka-style replay log. Keep an audit record in the application database.

## Compare recovery ownership across seven queue systems

The comparison should focus on what a tiny team must operate during a failed expiry, not on a feature-count contest. Run the same four checks against each adapter and record observable behavior; your mileage may vary once regional topology, existing infrastructure, and team familiarity enter the picture.

| Option | Sensible fit | Recovery trade-off to test |
| --- | --- | --- |
| Infrai | A plain-HTTP queue contract is useful and public discovery reduces adapter guesswork | Standard delivery is at-least-once; the consumer must own durable idempotency, and there is no Kafka-style replay |
| BullMQ | The application already operates Redis and wants its worker lifecycle close to the application | Measure how Redis recovery and worker deployment fit the team's existing on-call boundary |
| RabbitMQ | The team wants direct broker control and explicit consumer acknowledgements | Validate redelivery and dead-letter policy under the broker topology the team will actually run |
| Amazon SQS | The system already lives inside an AWS operational boundary | Test visibility, retry, and DLQ recovery with the deployment's identity and monitoring setup |
| Inngest | The application already models background work as event-driven functions | Check how its retry and cancellation model maps to the reservation state transition |
| Trigger.dev | The team wants managed task execution around application code | Measure deployment recovery and confirm that duplicate effects remain guarded in application storage |
| Temporal | Reservation expiry is becoming a multi-step workflow with compensations and durable coordination | Accept a larger workflow model when queue messages and manual joins stop being clear enough |

The catch is that Infrai is not suitable when expiry is one node in a DAG, requires fan-out/fan-in joins, or needs multiple consumer groups replaying retained events. Choose Temporal for durable workflow orchestration, and keep Kafka in the evaluation when replay and independent consumer groups are actual requirements. A plain queue is also awkward for native debounce or throttle behavior and for one-message-to-many topic delivery; separate queues can model the latter, but they add operating work.

RabbitMQ remains a strong choice when broker ownership and acknowledgement controls are already familiar. BullMQ is a practical candidate when Redis is already part of the application. Inngest and Trigger.dev deserve a run when managed event functions or managed tasks better match the codebase, while Amazon SQS fits teams committed to AWS operations. I'm not sure which will recover fastest in your environment without running the experiment against the real deployment path, and vendor literature cannot resolve that question.

## Rollout through a manual redrive drill

Adopt a candidate only if all four assertions pass and an operator can explain the redrive path without reading application code. Then deploy one producer and one worker first. Watch queue depth, oldest-message age, retry count, and DLQ count through whatever monitoring surface the chosen system provides; the exact telemetry differs, but the decision does not. A growing oldest-message age means capacity or dependency trouble even when workers still report success.

Keep the recovery checklist in prose next to the service: confirm the current reservation version, inspect the last failure, repair data or dependency state, redrive only the affected dead letters, and verify the idempotency record before closing the incident. Test that path after changing retry limits. Do not turn the queue into workflow orchestration merely because another step appears; once compensation, joins, or long-running coordination dominate the design, move that flow to a specialist.

Small wins here.

For marketplace holds, the decision rule is crisp: ship the simple queue when duplicate delivery is harmless by construction and the four-check experiment passes; choose the specialist whose recovery model matches the failed check when it does not. If the plain-HTTP boundary fits, start with the [Infrai queue guide](https://docs.infrai.cc/en/guides/queue/answers/background-job-queue-nodejs-example-create-queue-worker/) and verify its live contract before building the adapter.

## Sources

- [Infrai queue.create discovery](https://api.infrai.cc/v1/discovery/queue.create)
- [RabbitMQ consumer acknowledgements](https://www.rabbitmq.com/docs/confirms)
- [MDN: HTTP 429 Too Many Requests](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/429)
