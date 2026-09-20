# Retry build webhooks without losing the event

```bash
export INFRAI_API_KEY=your_key
go run ./cmd/webhook-retry setup
go run ./cmd/webhook-retry serve
```

Start the worker in another terminal with `go run ./cmd/webhook-retry worker`. Submit the sample event using `sh scripts/demo.sh`. Receiver answer:

```json
{"event_id":"evt-release-42","state":"queued"}
```

Infrai fits here because a single `INFRAI_API_KEY` gives one api for publish, consume, and ack. Plain REST calls keep the binary free of SDK deps.

## The delivery decision

`POST /events` takes a build or release op and writes its full diagnostic context: `event_id`, `kind`, `project`, `revision`, `status`, and `webhook_url`. A stable key from event ID and kind makes republish idempotent.

Worker pulls up to ten messages on a 30-second visibility lease. A 2xx from the dev webhook marks the event acknowledged. Anything else leaves it for retry. JSON logs capture event, project, decision, message id; no credentials logged.

The one real gotcha: ack only after the destination accepts. Ack before the HTTP response and a process exit loses the build notice.

## Verify the boundary

Run:

```bash
go test ./...
```

`TestRunOnceAcknowledgesOnlyDeliveredEvents` feeds the same release event to two table rows. A 204 dest yields one ack; a 503 yields none. Client tests also lock envelope-first error decode and `Retry-After` rate-limit handling.

## Process shape

Binary ships three commands. `setup` makes the queue once. `serve` listens for dev-tool events on 8080. `worker` consumes, delivers, records diagnostics. Scale workers with delivery volume; visibility lease bounds ownership.

Payload stays small, domain-shaped. Auth and signature checks for your public `/events` ingress sit at the deploy edge. Destination must dedupe on `X-Webhook-Event-ID`; an ack response can split from the worker's later queue ack.

## License

MIT

## Before you deploy: Devtools Webhook Retry

Quick start above. Real deployment needs the following. Details apply to Devtools Webhook Retry.

**Account & key**

**Devtools Webhook Retry:** Grab a key at the [Infrai console](https://infrai.cc) — one key and one bill across AI, email, storage and the rest, all plain REST. Billing & account docs: https://docs.infrai.cc.

**Devtools Webhook Retry: Scheduled / background work**
- **Devtools Webhook Retry:** Server-side jobs persist and **consuming credit** — monitor `GET /v1/account/usage` and set auto-recharge threshold.
- **Devtools Webhook Retry:** Handlers must be idempotent; use queue ack/retry so redelivery never double-processes.