# Fluxi Final Architecture

## 1. Goal

Fluxi is the reusable workflow runtime used to orchestrate checkout in the shopping-cart system without changing the course-provided HTTP API. The public contract stays synchronous, but the distributed coordination is moved into a Temporal-inspired engine.

The final design is shaped by three constraints:

* keep `POST /orders/checkout/{order_id}` unchanged
* survive single-container failures
* reduce hot-path orchestration overhead enough to make the benchmark viable

## 2. Final System Shape

### 2.1. Business Services

The application is split into three business domains:

* `order-service`
* `stock-service`
* `payment-service`

Each domain owns its own Redis-backed business state. For the `medium` and `large` deployments, each business Redis runs as:

* one master
* one replica
* three Sentinels

The business services are FastAPI applications served with Gunicorn/Uvicorn workers. They expose the original course-facing endpoints unchanged.

### 2.2. Fluxi Runtime

Fluxi consists of:

* `fluxi-server`
* `fluxi-scheduler`
* workflow workers
* activity workers
* Fluxi Redis master/replica/Sentinel

Fluxi Redis is the durable store for workflow execution metadata:

* workflow snapshots
* workflow history
* workflow task queues
* activity task queues
* timers and retry state
* sticky routing metadata

Workers no longer use `fluxi-server` for the hot path. They talk directly to the Fluxi Redis store for:

* history fetches
* workflow-task completion
* activity-task completion

`fluxi-server` remains the client-facing control plane for:

* workflow start/attach
* result waiting
* readiness and administrative access

### 2.3. Gateway

The gateway is Nginx. In `medium` and `large`, it uses explicit upstream pools with `least_conn` and failover settings instead of relying on Docker DNS for a single service name.

That gives real balancing across API replicas for:

* `order-service`
* `stock-service`
* `payment-service`

Important operational note:

* this gateway setup is robust for kill/start testing on existing containers
* if API containers are recreated and get new IPs, the gateway should be restarted so upstream name resolution is refreshed

## 3. Checkout Execution Model

### 3.1. Public API

External checkout is still synchronous:

1. client calls `POST /orders/checkout/{order_id}`
2. `order-service` prepares workflow input from business state
3. `order-service` executes a Fluxi workflow and waits for the terminal result
4. the HTTP response is mapped back to the original API contract

There is no API contract change and no asynchronous callback/polling API.

### 3.2. Checkout Attempt Model

The original single-key checkout model was not sufficient because a later retry after a terminal failure must be able to create a new workflow run, while duplicate concurrent calls should attach to the existing in-flight attempt.

The final design uses explicit checkout attempts:

* workflow id: `order-checkout:{order_id}:attempt:{n}`
* start policy: `ATTACH_OR_START`

This gives the desired behavior:

* duplicate or retried HTTP requests while attempt `n` is running attach to the same workflow
* a later legitimate retry after a terminal business failure can start attempt `n+1`

### 3.3. Happy Path

The checkout workflow is:

1. reserve stock on the `stock` activity queue
2. charge payment on the `payment` activity queue
3. mark the order as paid using a local activity
4. complete the workflow

The final `mark_order_paid` step is intentionally a local activity. That removes:

* one extra remote activity task
* one extra workflow wakeup
* one extra round through the `orders` activity queue

So the happy path now completes in three order workflow tasks instead of four.

## 4. Important Design Decisions

### 4.1. Hybrid State Model

Fluxi uses snapshot plus history:

* a current workflow snapshot for fast reads and state advancement
* append-only durable history for replay and recovery

This was chosen over pure event sourcing because the benchmark hot path benefits from fast current-state reads.

### 4.2. Real Sticky Execution

Sticky routing is not just a sticky queue name. The worker now keeps a per-run in-memory workflow session cache containing:

* workflow instance
* live coroutine/session state
* pending activity futures
* last applied history index

On a sticky hit, the worker fetches only incremental history and resumes the live session. On a miss, timeout, or failover, it rebuilds deterministically from durable history.

This was a major design choice because the original replay-every-turn approach was structurally too expensive.

### 4.3. Direct Store Worker Hot Path

Workers use the Fluxi Redis store directly instead of making an HTTP round-trip through `fluxi-server` for each workflow turn.

This was done because:

* the workflow path is latency-sensitive
* repeated HTTP control-plane hops were dominating checkout latency
* `fluxi-server` is still useful for client start/result traffic, but not for worker hot-path execution

### 4.4. General Local Activities

Fluxi now supports both remote and local activities.

Local activities:

* run inside the workflow worker
* are recorded in workflow history
* do not create Redis activity tasks
* do not require a follow-up workflow task just to consume their result

The shopping-cart workflow uses this for `mark_order_paid`, but the capability is general rather than checkout-specific.

### 4.5. At-Least-Once Delivery With Idempotency

The system does not assume exactly-once execution.

Instead:

* task delivery is at least once
* every activity uses a stable `activity_execution_id`
* duplicate effects are prevented with durable idempotency checks
* stale completions are fenced by `attempt_no`

This design is much more defensible than trying to simulate exactly-once semantics on top of Redis and container restarts.

### 4.6. Retry Ownership

Retry ownership is split deliberately:

* Fluxi owns workflow-task and activity-task retries, timeout handling, and stale-attempt fencing
* business services own bounded retry of transient Redis/Sentinel failover errors when accessing their own domain Redis
* `order-service` owns bounded retry of ambiguous workflow result decoding during failover on the same checkout attempt

This keeps the engine responsible for orchestration while still letting service endpoints degrade into slower success instead of leaking raw `500`s during Redis failover.

### 4.7. Sentinel Instead of Redis Cluster

The project uses master/replica Redis with Sentinel, not Redis Cluster.

Reasoning:

* simpler to reason about in Docker Compose
* easier to keep queue and state transitions atomic
* sufficient for the course deployment model

Trade-off:

* failover is not zero-downtime
* recent writes can still be lost during crash/failover windows
* horizontal write scaling is not provided

## 5. Runtime Roles

The codebase is shared, but runtime roles are separated.

### 5.1. API Roles

* `order-service`
* `stock-service`
* `payment-service`

Responsibilities:

* serve HTTP requests
* validate and map inputs/outputs
* call Fluxi or local repositories
* remain thin with respect to orchestration

### 5.2. Workflow Worker Role

* `order-checkout-worker`

Responsibilities:

* poll workflow tasks
* rebuild or resume workflow sessions
* execute deterministic workflow code
* emit workflow commands
* run local activities inline

### 5.3. Activity Worker Roles

* `stock-activity-worker`
* `payment-activity-worker`

Responsibilities:

* poll activity queues
* execute business operations
* deduplicate duplicate deliveries
* report results back to Fluxi

### 5.4. Fluxi Control Plane

* `fluxi-server`
* `fluxi-scheduler`

Responsibilities:

* workflow start/attach decisions
* result waiting
* timer expiry
* retry scheduling
* stale-entry cleanup

## 6. High Availability Model

### 6.1. Small Profile

The `small` profile is intentionally simple:

* single instance of each API
* single instance of each worker role
* single instance of each control-plane component
* single Redis instance per domain

It is meant for basic correctness and local development, not high availability.

### 6.2. Medium and Large Profiles

The `medium` and `large` profiles are the HA-shaped deployments.

They include:

* replicated API services
* replicated Fluxi servers
* replicated workflow/activity workers
* two scheduler replicas
* Fluxi Redis master/replica/3 Sentinels
* order Redis master/replica/3 Sentinels
* stock Redis master/replica/3 Sentinels
* payment Redis master/replica/3 Sentinels

## 7. Resilience Strategy

### 7.1. Worker and Service Failures

For worker and stateless service failures, the target behavior is:

* no permanent outage
* transient latency spike
* automatic recovery after the container returns

With the final gateway and retry changes, low-load HA testing showed clean recovery for:

* `order-service`
* `order-checkout-worker`
* `payment-activity-worker`
* `fluxi-server`

### 7.2. Redis Master Failover

Redis master failover is handled through:

* Sentinel promotion
* Redis client reconnection
* bounded failover retries in business services
* bounded retry on ambiguous checkout result decoding

The design goal is not zero-latency failover. The goal is:

* turn many potential `500`s into slower `200`s or controlled temporary unavailability
* avoid duplicate business effects

HA testing showed that after the final fixes:

* `order-db-master` failover degraded mostly into slower success
* `stock-db-master` failover degraded into slower success once the gateway was restarted after API container recreation
* `payment-db-master` failover also degraded into slower success
* `fluxi-redis-master` failover caused large latency spikes but no persistent failure at low load

### 7.3. Readiness

The services expose readiness endpoints and the Compose profiles use health checks so traffic is not sent to freshly started containers immediately.

This matters because Redis and Fluxi components can be `Up` before they are actually ready to serve traffic safely.

## 8. Performance-Oriented Choices

The biggest performance improvements over the earlier design were:

* direct Redis/store hot path for workers
* real sticky workflow session reuse
* local activity support
* explicit gateway balancing for API replicas
* tuned long-poll result waiting and connection pooling

The current synchronous external checkout still means internal queueing shows up directly as user-visible latency. That is a conscious choice because the public API contract cannot change.

## 9. Known Limitations

The final system is materially stronger than the original baseline, but it still has real limitations:

* Redis Sentinel failover is not zero-error and not zero-latency in the strict sense
* recent writes can still be lost because replication and AOF are asynchronous
* the gateway should be restarted after API container recreation so upstream IP resolution is refreshed
* the public checkout API is synchronous, so failover and queueing directly affect HTTP latency
* the design is defensible for course evaluation, but not intended as a production-grade Temporal replacement

## 10. Final Summary

The final architecture is a Temporal-inspired workflow runtime with:

* durable workflow state and history in Redis
* deterministic replay
* direct-store worker execution
* sticky in-memory workflow sessions
* local and remote activities
* attempt-based checkout start semantics
* service and database HA for `medium` and `large`
* explicit gateway balancing and failover behavior
* bounded retries to convert many transient failover errors into slower successful requests

This is the architecture that is actually implemented and benchmarked in the repository.
