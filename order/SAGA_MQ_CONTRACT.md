# Saga MQ Contract (Order-Orchestrated)

This document defines the Redis Streams contract used by the `order` Saga orchestrator.

## Broker

- Redis instance from compose service: `saga-broker`
- `order` publishes commands to participant command streams.
- Participants must publish a single response to the `reply_stream` from each command.

## Command Streams

- Stock commands: `saga:cmd:stock`
- Payment commands: `saga:cmd:payment`

## Command Message Fields

- `operation_id`: unique per command attempt
- `tx_id`: saga transaction id
- `participant`: `stock` or `payment`
- `action`:
  - Stock: `reserve`, `release`
  - Payment: `debit`, `refund`
- `reply_stream`: stream name where participant must send the response
- `payload`: JSON object string
- `sent_at_ms`: unix epoch millis

Payload shapes:

- `payment.debit`: `{"user_id":"<id>","amount":<int>}`
- `payment.refund`: `{}`
- `stock.reserve`: `{"item_id":"<id>","amount":<int>}`
- `stock.release`: `{"item_id":"<id>","amount":<int>}`

## Response Message Fields (required)

- `ok`: `1|0` (or `true|false`)
- `retryable`: `1|0` (or `true|false`)
- `detail`: optional error string

The orchestrator treats:

- `ok=true` as step success
- `ok=false,retryable=true` as retryable failure
- `ok=false,retryable=false` as terminal failure

## Participant Requirements

- Idempotent handling by `(tx_id, action, payload)` semantics.
- Durable records so restart recovery can re-emit consistent responses.
- Exactly one logical response per command (duplicate responses are tolerated but unnecessary).
- Response should be written to the provided `reply_stream`.

## Local Run

- 2PC mode: `TX_MODE=2pc`
- Saga mode (MQ): `TX_MODE=saga`
