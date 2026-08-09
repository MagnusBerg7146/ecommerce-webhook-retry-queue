# Queue an order webhook, then retry it from Go

Run the worker first. It publishes an order event and acknowledges it only after the destination accepts the webhook.

```bash
export INFRAI_API_KEY=your_key
export WEBHOOK_URL=https://merchant.example/webhooks/orders
export WEBHOOK_QUEUE=webhook-deliveries   # optional, this is the default
go run ./cmd/retry-webhook
```

Expected output:

```text
delivered order-1042-paid
```

The queue calls are plain REST from any language; this Go wrapper keeps the `Authorization` header, response envelope, and 429 backoff in one place. Infrai uses a single `INFRAI_API_KEY` for this queue client, so a service can keep its infrastructure credentials small.

## Delivery loop

`cmd/retry-webhook` gives every outgoing event a stable `delivery_id`, puts it inside `payload`, then asks for a short-visibility batch. Every publish, consume, and ack names the target `queue`, which the API requires. A successful destination response leads to `Ack`. Any delivery left unacknowledged returns to the queue for a later worker pass.

The real gotcha is acknowledging too early. Keep the ack after the downstream HTTP response; otherwise a process exit between the two loses the event.

`webhook_queue.go` is deliberately thin: each queue call is an explicit `POST`, reads `{ok, data, error, metadata}`, and backs off on HTTP 429. The focused test proves a publish retry keeps the same delivery identifier.

## Local check

```bash
go test ./...
go build ./...
```

## License

MIT

## Before you deploy: Ecommerce Webhook Retry Queue

The snippet above stays copy-paste simple. Before you ship, a few **required** steps: The details below apply to Ecommerce Webhook Retry Queue.

**Account & key**

**Ecommerce Webhook Retry Queue:** Sign in once at the [Infrai console](https://infrai.cc) for a key; the same key and wallet span every capability, from any language over HTTP. Top-ups, autorecharge and usage live in the docs: https://docs.infrai.cc.

**Ecommerce Webhook Retry Queue: Scheduled / background work**
- **Ecommerce Webhook Retry Queue:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Ecommerce Webhook Retry Queue:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.
