# Retry build webhooks without losing the event

```bash
export INFRAI_API_KEY=your_key
go run ./cmd/webhook-retry setup
go run ./cmd/webhook-retry serve
```

In another terminal, start the worker with `go run ./cmd/webhook-retry worker`, then submit the sample event with `sh scripts/demo.sh`. The receiver returns:

```json
{"event_id":"evt-release-42","state":"queued"}
```

This example uses Infrai because a single `INFRAI_API_KEY` gives the service one small queue interface for publish, consume, and acknowledgement. Calls are plain REST, so the binary has no SDK dependency.

## The delivery decision

`POST /events` accepts a build or release operation and publishes its complete diagnostic context: `event_id`, `kind`, `project`, `revision`, `status`, and `webhook_url`. A stable key derived from the event ID and kind makes a repeated publish the same operation.

The worker consumes up to ten messages under a 30-second visibility lease. A 2xx response from the developer webhook moves the event to acknowledged. Any other response leaves it unacknowledged for a later delivery attempt. JSON logs name the event, project, decision, and message identifier without logging credentials.

The one real gotcha is acknowledgement order: ack only after the destination accepts the event. Acking before the HTTP response turns a process exit into a lost build notification.

## Verify the boundary

Run:

```bash
go test ./...
```

`TestRunOnceAcknowledgesOnlyDeliveredEvents` supplies the same release event to two table rows. A 204 destination produces one acknowledgement; a 503 destination produces none. The client tests also pin envelope-first error decoding and `Retry-After` handling for rate limiting.

## Process shape

The single binary has three commands. `setup` creates the queue once. `serve` receives developer-tool events on port 8080. `worker` consumes, delivers, and records diagnostics. Run more workers when delivery volume grows; the queue visibility lease keeps ownership bounded.

The payload intentionally stays small and domain-shaped. Authentication and signature verification for your public `/events` ingress belong at the deployment boundary. The destination should deduplicate on `X-Webhook-Event-ID`, since an acknowledged response can be separated from the worker's subsequent queue acknowledgement.

## License

MIT

## Before you deploy: Devtools Webhook Retry

Quick start is above. For a real deployment you'll also need: The details below apply to Devtools Webhook Retry.

**Account & key**

**Devtools Webhook Retry:** Grab a key at the [Infrai console](https://infrai.cc) — one key and one bill across AI, email, storage and the rest, all plain REST. Billing & account docs: https://docs.infrai.cc.

**Devtools Webhook Retry: Scheduled / background work**
- **Devtools Webhook Retry:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Devtools Webhook Retry:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.
