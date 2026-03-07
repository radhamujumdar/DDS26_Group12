# Saga MQ Contract v1 (Order-Orchestrated)

This file defines the queue contract for Saga mode.

## Broker

- Redis Streams broker: `saga-broker`
- Message schema version: `v1`

## Stream Topology

Partition count is configurable (`SAGA_MQ_STREAM_PARTITIONS`, default `4`).
Partition selection is deterministic in `order`: `partition = sha256(tx_id) % P`.

### Command streams (written by `order`, consumed by participants)

- Stock: `saga:cmd:stock:p{partition}`
- Payment: `saga:cmd:payment:p{partition}`

### Result streams (written by participants, consumed by `order`)

- Stock: `saga:res:stock:p{partition}`
- Payment: `saga:res:payment:p{partition}`

## Command Envelope (required fields)

- `schema_version` = `v1`
- `msg_id` (unique id for this command attempt)
- `correlation_id` (unique id used to match result to pending command)
- `tx_id`
- `participant` (`stock` | `payment`)
- `action`
- `partition`
- `attempt`
- `sent_at_ms`
- `payload` (JSON string)

Payload shapes:

- payment `debit`: `{"user_id":"<id>","amount":<int>}`
- payment `refund`: `{}`
- stock `reserve`: `{"item_id":"<id>","amount":<int>}`
- stock `release`: `{"item_id":"<id>","amount":<int>}`

## Result Envelope (required fields)

- `schema_version` = `v1`
- `correlation_id`
- `tx_id`
- `participant`
- `action`
- `ok` (`1|0` or `true|false`)
- `retryable` (`1|0` or `true|false`)
- `detail` (optional)
- `processed_at_ms`

## Semantics

- `ok=true`: step success.
- `ok=false,retryable=true`: retryable failure, orchestrator may retry.
- `ok=false,retryable=false`: terminal participant failure for this attempt.

## Order-side durability keys

- Pending commands: `saga:mq:pending:{correlation_id}`
- Dispatcher cursors: `saga:mq:cursor:{participant}:p{partition}`

## Participant requirements (to be implemented)

- Consumer group workers over command streams.
- Durable idempotency by business key:
  - payment: `(tx_id, action)`
  - stock: `(tx_id, item_id, action)`
- Ack command only after durable write and result publication.
- Replay-safe behavior on duplicate command delivery.

## Local modes

- 2PC mode: `TX_MODE=2pc`
- Saga mode (MQ): `TX_MODE=saga`
