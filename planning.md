# Requirements

## 1. Functional Requirements

### 1.1. Fluxi Orchestrator Engine

* **Durable Workflow State:** Fluxi must durably track active, completed, and failed workflows.
* **Durable Workflow History:** Fluxi must persist workflow events so a workflow worker can replay the history after a crash and rebuild deterministic local state.
* **Logical Task Queues:** Fluxi must maintain distinct logical queues for workflow tasks and activity tasks.
* **Task Dispatching:** Fluxi must assign pending activities to available workers listening on the corresponding queue.
* **State Advancement:** When a workflow task or activity result arrives, Fluxi must append the event, update the workflow snapshot, and schedule the next step.
* **Timeout Handling:** Fluxi must detect missing activity completions and mark the activity attempt as timed out.
* **Retry Handling:** Fluxi should own activity retries, backoff, and retry limits. Upstream clients should only retry idempotent workflow start requests when the initial result is ambiguous.
* **Idempotent Workflow Start:** Starting a workflow with the same stable workflow key must be safe to retry.
* **Attempt Fencing:** Every workflow-task attempt and activity-task attempt must carry an `attempt_no` (or equivalent token). Fluxi must accept a completion only if it matches the currently open attempt and must ignore stale late completions from superseded attempts.

### 1.2. SDK and Worker Runtime

* **Declarative Registration:** The SDK must let developers register Python workflows and activities explicitly.
* **Workflow Replay:** Workflow execution must resume by rerunning deterministic workflow code against durable event history until it reaches the same logical point again. It must not depend on preserving Python stack frames in memory or on arbitrary coroutine metaprogramming.
* **Execution Interception:** When workflow code schedules an activity, the SDK must not run the function locally. It must emit an activity command to Fluxi instead.
* **Background Polling:** Worker processes must continuously poll Redis queues managed by Fluxi for workflow tasks or activity tasks.
* **Result Reporting:** After an activity finishes, the worker must report success or failure back to Fluxi.
* **Deterministic Workflow Rules:** Workflow code must avoid nondeterministic operations such as direct network I/O, direct database access, uncontrolled randomness, or wall-clock time without going through Fluxi APIs.

### 1.3. Shopping Cart Integration

* **Stable External API:** The public `order`, `stock`, and `payment` HTTP APIs provided by the course template must remain unchanged.
* **FastAPI Runtime:** The template implementation will be rewritten on FastAPI while preserving the externally visible API contract.
* **Checkout via Fluxi:** `POST /orders/checkout/{order_id}` must start or resume a Fluxi workflow internally instead of coordinating the transaction directly in application code.
* **Synchronous Client Experience:** The external checkout endpoint must still wait for a terminal workflow result so the provided tests and benchmark can keep the same request-response contract.
* **Checkout Attempt Semantics:** A new `/orders/checkout/{order_id}` call after a retryable business failure must be able to start a fresh checkout attempt for the same order. A concurrent or duplicate call while checkout is already in progress must attach to the existing in-flight attempt instead of creating a second one.
* **No Direct Coordination Between Microservices:** Business services must not call each other directly to coordinate checkout.

## 2. Non-Functional Requirements

### 2.1. Fault Tolerance and Resilience

* **Worker Crash Recovery:** If a worker dies mid-task, the task must become available for recovery and re-execution by another healthy worker.
* **Orchestrator Crash Recovery:** If a Fluxi server crashes, another server instance must reconstruct workflow progress from Redis state and history.
* **At-Least-Once Delivery:** Task delivery is at-least-once. Exactly-once execution is not assumed.
* **Idempotent Activities:** Every activity must be safe under duplicate delivery by using a stable `activity_execution_id` and durable deduplication.
* **Ambiguous Start Recovery:** If a workflow start succeeds but the caller does not know whether the acknowledgment was durably recorded, retrying the same stable workflow key must be safe.
* **Stale Completion Safety:** If a timed-out worker reports a late result after a newer retry has already started, Fluxi must ignore that stale result.

### 2.2. Scalability and Performance

* **Horizontal Worker Scaling:** Each worker type must support multiple replicas.
* **Competing Consumers:** Tasks in a queue must be distributed across worker replicas with Redis Streams consumer groups.
* **Stateless Fluxi Servers:** Fluxi server instances must be horizontally scalable because durable state lives in Redis, not in process memory.
* **Async Internal Communication:** Internal coordination must be event-driven and non-blocking.
* **Connection Pooling**: All API nodes, Fluxi servers, and workers maintain persistent async Redis connection pools (redis.asyncio.ConnectionPool) to prevent TCP exhaustion at high throughput.

### 2.3. Maintainability and Developer Experience

* **Workflows as Code:** Developers should express workflow coordination in normal Python control flow.
* **Separation of Concerns:** Workflow coordination belongs in Fluxi. Business logic belongs in the microservices.
* **Operational Simplicity:** Phase 2 should prefer a simpler, defensible architecture over a more ambitious but fragile one.

# Architecture Overview

## 1. Executive Summary

Fluxi is a Temporal-inspired workflow engine designed as a reusable orchestration runtime. For Phase 2 of the DDS project, it will be used to move distributed coordination out of the `order`, `stock`, and `payment` application code and into that runtime.

The system keeps the course-facing HTTP API unchanged, but replaces direct service-to-service coordination with:

* FastAPI-based HTTP services
* durable workflow state and event history in Redis
* workflow and activity task queues in Redis Streams
* replay-based workflow execution
* idempotent activity execution
* multi-worker services for load balancing and crash recovery

Fluxi is not trying to be a full Temporal replacement. The goal is a focused, defensible workflow engine with reusable execution primitives, validated through the shopping-cart workflow and the course fault-injection benchmark.

## 2. Final Design Decisions

### 2.1. Storage and HA

We will use a **single logical Redis deployment** as Fluxi's only state store:

* one writable Redis master
* one or more Redis replicas
* Redis Sentinel for failover
* AOF enabled with `appendfsync everysec`

This choice is intentionally pragmatic:

* simpler than Redis Cluster
* keeps state updates and queue appends in the same datastore
* allows atomic state transition plus task enqueue operations
* good enough for the project timeline

Known limitation:

* recent writes can still be lost during crash or failover because AOF and replication are not synchronous

Mitigations:

* stable workflow IDs
* idempotent activity execution
* replay from durable history
* safe retry of ambiguous workflow start requests

### 2.2. No Redis Cluster in v1

We are **not** using Redis Cluster in the first version.

Reasoning:

* Cluster adds substantial operational complexity in Docker and local development.
* Hash tags help colocate per-workflow keys, but they do not automatically solve global queue design.
* Atomic `update workflow state + append next task` becomes more complicated once the state key and queue key can end up on different shards.
* The project is more likely to bottleneck on application logic and worker throughput before one Redis master becomes the limiting factor.

### 2.3. Hybrid State Model

Fluxi will use **hybrid snapshot + history**:

* a current workflow snapshot for fast reads
* an append-only event history for replay, debugging, and recovery

We are not using pure event-sourcing in v1 because replaying the full history for every operational read adds complexity and latency without enough benefit for this project.

### 2.4. Internal Retries, Not App-Level Coordination

Fluxi will own **activity retries, timeouts, and retry policy**.

Upstream retry is still useful, but only for ambiguous workflow-start requests such as:

* a client called `StartWorkflow(workflow_key, workflow_type, input...)`
* the network failed before the caller knew whether the start was committed

The caller should be able to retry the same stable `workflow_key` safely. It should not re-implement workflow retry logic itself.

Fluxi itself owns activity timeout detection and retry scheduling. Workers should stay operationally simple and consume only new tasks.

Fluxi should distinguish between:

* the workflow key: stable caller-supplied logical identity used for idempotent start-or-attach
* the run number: monotonically increasing execution sequence for that workflow key
* the concrete execution identity: `run_id`
* the task delivery attempt: `attempt_no`

The important rule is that one logical workflow key can have multiple runs over time, but only one open run at a time in v1.

Fluxi, not application code, should therefore own a generic execution state machine, for example:

* `absent`
* `running`
* `completed`
* `failed`

Business-specific meanings such as `paid`, `rejected`, or `failed_retryable` belong in workflow results or application-level projections, not in the engine's core execution state.

Expected behavior:

* if `execute_workflow(workflow_key, ...)` arrives while a run is already `running`, Fluxi should attach the caller and wait for that run's terminal result
* if start policy permits a new run after a terminal result, Fluxi should create run `n+1`
* if idempotency policy says the prior terminal result should be reused, Fluxi should return it without creating a second concurrent run

For the shopping-cart integration, the order API should stay thin:

* load the business order data needed as workflow input
* call a Fluxi `start_or_attach` / `execute_workflow` API using `workflow_key = checkout:{order_id}`
* wait for the terminal workflow result
* map that result back to the existing HTTP response contract

This avoids a crash window where application code records engine lifecycle state before Fluxi durably creates the workflow, or where Fluxi finishes the workflow before the application durably records any domain projection it wants to maintain.

### 2.5. Separate API and Worker Roles

We will use **multi-worker services**, but worker loops will not run inside the same FastAPI / ASGI web-server processes that serve HTTP traffic.

Instead, each business domain will expose separate runtime roles:

* API role: serves the existing HTTP endpoints
* workflow-worker role: executes workflow code by replaying history
* activity-worker role: executes business activities

The codebase can still be shared, but the runtime roles should be deployed as separate processes or containers.

### 2.6. Scope of Fluxi v1

Fluxi v1 is intended to be a reusable workflow runtime. For this course, the shopping-cart system is the first integration target and validation workload.

It aims to:

* abstract the coordination logic previously embedded in the services
* provide reusable workflow and activity execution primitives
* expose a clean model for start-or-attach, replay, retries, and durable history
* demonstrate the engine by powering the checkout workflow cleanly and defensibly

It does **not** aim to:

* reproduce all of Temporal's features
* expose a complete generic library of every distributed transaction protocol
* optimize for production-grade multi-tenant scale in the first version

## 3. System Architecture

### 3.1. Layer 1: Redis Persistence and Routing

Redis is the single logical durable backend for Fluxi.

Redis responsibilities:

* workflow snapshots
* workflow event history
* workflow task queues
* activity task queues
* timer and retry scheduling
* leases or attempt bookkeeping

Suggested Redis structures:

* `workflow:{run_id}:state` -> hash with current snapshot for one concrete workflow run
* `workflow:{run_id}:history` -> stream or list of durable events for one concrete workflow run
* `queue:workflow:{task_queue}` -> stream of workflow tasks
* `queue:activity:{activity_queue}` -> stream of activity tasks
* `timers` -> sorted set for retry and timeout deadlines
* `activity:{execution_id}` -> deduplication record plus current accepted attempt metadata
* `workflow_key:{workflow_key}:control` -> Fluxi-owned control record with current `run_no`, active `run_id`, terminal status, and start/attach metadata

Outside Fluxi itself, applications should persist their own domain state and any idempotent projections they need, but Fluxi should remain the source of truth for workflow execution lifecycle metadata.

### 3.2. Layer 2: Fluxi Server

Fluxi server is a stateless Python FastAPI service that owns orchestration metadata and scheduling decisions.

Responsibilities:

* atomically decide `start new run` vs `attach to running run` vs `return existing terminal result`
* start workflows
* append workflow events
* maintain workflow snapshot
* own execution metadata such as current `run_no`, active `run_id`, and terminal status
* schedule workflow tasks and activity tasks
* process activity results
* evaluate retry policy and timeout policy
* finalize workflow outcomes and expose them to callers idempotently
* expose SDK-facing APIs

A Fluxi server does **not** keep workflow progress in memory as the source of truth. Redis remains authoritative.

### 3.3. Layer 3: Workflow Workers

Workflow workers execute user-defined workflow code.

Responsibilities:

* poll workflow tasks from Redis queues managed by Fluxi
* load and replay workflow history
* run deterministic workflow code until the next command or terminal result
* emit commands such as `schedule_activity`, `complete_workflow`, or `fail_workflow`

This is the part that is intentionally Temporal-like:

* workflow code is written by developers
* workflow code is executed by workers
* replay rebuilds local workflow state after crashes
* Fluxi server stores history and schedules tasks into Redis queues

Workflow workers should stay dumb with respect to orchestration bookkeeping:

* they do not decide whether a caller should attach to an existing run or start a new run
* they do not own retry counters, timeout policy, or stale-attempt fencing
* they simply replay deterministic code and emit the next command

### 3.4. Layer 4: Activity Workers

Activity workers run user-defined activity logic.

Responsibilities:

* poll activity tasks from Redis Streams
* execute domain logic
* persist local service changes
* report activity result back to Fluxi
* deduplicate duplicate deliveries using `activity_execution_id`

Each activity worker type can have multiple replicas.

Activity workers should also stay operationally simple:

* they do not decide retry policy or timeout policy
* they do not coordinate with other services directly
* they execute one task, persist an idempotent result, and report back to Fluxi

### 3.5. Runtime Stack

The implementation stack for Phase 2 is:

* FastAPI for all HTTP-facing services
* ASGI server processes for API roles
* dedicated Python worker entrypoints for workflow and activity workers
* shared domain modules so API and worker roles reuse the same business logic where appropriate

## 4. Execution Model

### 4.1. Workflow Start

1. A caller invokes a Fluxi API such as `execute_workflow(workflow_key=..., workflow_type=..., input=...)`.
2. Fluxi atomically loads the control record for that `workflow_key` and decides one of:
   * `attach` to the currently running run
   * `return existing terminal result` if start policy says the existing terminal run should be reused
   * `start new run n+1` if no run exists yet or if start policy allows a new run after the previous terminal state
3. If a new run is needed, Fluxi atomically:
   * increments `run_no`
   * allocates a fresh `run_id`
   * updates `workflow_key:{workflow_key}:control` to `running`
   * creates the initial workflow snapshot for that `run_id`
   * appends a `WorkflowStarted` history event for that `run_id`
   * enqueues the first workflow task
4. Fluxi waits for or later returns the terminal result for the active run.
5. Fluxi durably records the terminal workflow outcome in its control record before exposing that result to callers.

If the same logical workflow is started again, the operation must be idempotent at the level of the stable `workflow_key`. The important point is that the caller supplies the stable logical identity, while Fluxi derives a concrete `run_id` for each new run.

At the SDK level, `execute_workflow(...)` should therefore mean "start-or-attach using a stable workflow key, then await the terminal result", not "blindly create a brand new workflow run every time."

For the shopping-cart integration, the order API can simply call `execute_workflow(workflow_key=f"checkout:{order_id}", workflow_type=CheckoutWorkflow, input=...)` and then map the returned workflow result back to the existing HTTP response contract.

### 4.2. Workflow Replay

Workflow code does not resume from a suspended Python stack frame. Instead, Fluxi uses constrained replay of deterministic SDK-based workflow code:

1. a workflow worker receives a workflow task
2. it loads the durable event history
3. it reruns the workflow code from the beginning
4. when the workflow reaches a previously completed SDK command, Fluxi returns the recorded result from history instead of re-executing side effects
5. execution continues until the workflow reaches a new SDK command that must be scheduled, or reaches a terminal state
6. the worker then emits the resulting command back to Fluxi and stops

This is not a general-purpose replay engine for arbitrary Python programs. It only works for deterministic workflow code written against Fluxi SDK primitives.

This is why workflow code must be deterministic.

### 4.3. Activity Scheduling

When workflow code calls something like:

```python
await workflow.execute_activity("process_step", args=[resource_id])
```

the SDK does not execute that activity locally.

Instead, the workflow worker emits a command to Fluxi:

* append `ActivityScheduled` to history
* update the workflow snapshot
* allocate a stable `activity_execution_id` for the logical side effect
* allocate `attempt_no = 1` for the first delivery attempt
* append an activity task to the configured activity queue

These writes must happen atomically in Redis.

### 4.4. Activity Consumption

Activity workers use Redis Streams consumer groups:

```text
XREADGROUP GROUP activity-workers worker-1 BLOCK 5000 STREAMS queue:activity:default >
```

Important semantics:

* `>` means "give this consumer only brand new entries that have never been delivered to any consumer in this group"
* brand new entries move to the consumer's pending entries list
* a later `XACK` acknowledges successful handling
* pending entries are **not** returned again by another `XREADGROUP ... >`

This means stale pending messages require recovery logic.

### 4.5. Stale Task Recovery

Fluxi is an at-least-once system.

If a worker crashes after receiving a task but before acknowledging it, Fluxi handles recovery by timeout and explicit retry scheduling.

In the v1 design:

* workers only consume brand new tasks via `XREADGROUP ... >`
* workers do not aggressively reclaim stale pending entries themselves
* Fluxi tracks which attempt is currently valid for each outstanding workflow task or activity
* when an attempt times out, Fluxi marks that attempt as superseded and schedules retry attempt `n+1`
* every completion report must include both the logical task identity and the `attempt_no`
* Fluxi records a completion only if the reported `attempt_no` still matches the currently open attempt
* a maintenance loop in Fluxi may inspect and clean up abandoned pending entries so the PEL does not grow forever

The key point is:

* the retry decision is owned centrally by Fluxi
* worker code stays simple because retry ownership, stale-attempt fencing, and timeout handling all live in the engine
* stale original deliveries may still exist in the PEL even after a retry is scheduled
* duplicate execution is still possible at the worker level if a late worker reports after the retry has started
* Fluxi must ignore stale completions from superseded attempts instead of letting them advance the workflow twice

Therefore all activities must be idempotent, and Fluxi must fence stale attempts.

## 5. Failure Model and Recovery Strategy

### 5.1. Delivery Guarantee

Fluxi assumes **at-least-once** activity delivery.

Implications:

* an activity may run more than once
* a worker may commit domain state and crash before reporting completion
* the task may later be retried

So every activity must check whether `activity_execution_id` has already been applied.

### 5.2. Worker Crash

Scenario:

1. an activity worker receives a task
2. it updates application state
3. it crashes before `XACK` or before Fluxi records completion

Recovery:

* the activity remains pending or times out
* another worker reprocesses the same execution
* the duplicate is detected using the execution ID
* the worker returns the same logical result without applying the side effect twice
* if the original timed-out attempt later reports after a newer retry has started, Fluxi rejects that stale completion because its `attempt_no` is no longer current

### 5.3. Redis Crash and the AOF Window

With `appendfsync everysec`, Redis may lose up to roughly one second of recent writes.

This creates two different classes of failure:

* **duplicate-after-recovery:** a completion event or acknowledgment is lost, so the activity is retried
* **amnesia:** a just-started workflow or just-enqueued task disappears after crash

Idempotent workers solve the first case, but not the second.

Mitigation for amnesia:

* workflow start uses a stable `workflow_key`
* the caller can safely retry `StartWorkflow(workflow_key)` if the result is ambiguous
* Fluxi must treat repeated starts of the same `workflow_key` as idempotent while still deriving a distinct internal `run_id` for each new run when policy allows

This does not make the system perfect, but it makes the ambiguous-start case recoverable.

### 5.4. Sentinel Failover Limitations

Sentinel improves availability, but it does not provide:

* zero data loss
* synchronous replication
* exactly-once execution
* horizontal write scaling

So the design must still tolerate:

* duplicate deliveries
* stale reads during failover windows
* recent write loss

### 5.5. Orchestrator Crash Recovery

Because Fluxi servers are stateless, another server instance can continue orchestration by reading:

* workflow snapshot
* workflow history
* pending or scheduled tasks
* timers

No in-memory workflow state is authoritative.

## 6. Consistency and Atomicity

Within the single logical Redis deployment, Fluxi should use `WATCH/MULTI/EXEC` or Lua scripts for operations such as:

* append history event + update snapshot + enqueue next task
* record activity result only if `attempt_no` matches the current open attempt + enqueue next workflow task
* mark timeout, supersede attempt `n`, and schedule retry attempt `n+1`

Keeping state and queues in the same datastore is the main reason we prefer this design over splitting state and broker into separate independent systems for v1.

## 7. SDK Model

### 7.1. Workflow Definition

```python
from fluxi_sdk import workflow

@workflow.defn
class ExampleWorkflow:
    @workflow.run
    async def run(self, resource_id: str):
        metadata = await workflow.execute_activity("load_metadata", args=[resource_id])
        result = await workflow.execute_activity("process_resource", args=[resource_id, metadata])
        return {"status": "completed", "result": result}
```

Notes:

* this code is executed by workflow workers
* replay rebuilds the workflow's logical state
* the workflow code must stay deterministic

### 7.2. Activity Definition

```python
from fluxi_sdk import activity

@activity.defn(name="process_resource", queue="queue:activity:default")
async def process_resource(resource_id: str, metadata: dict, activity_execution_id: str):
    if resource_repo.already_applied(activity_execution_id):
        return resource_repo.result_for(activity_execution_id)

    result = resource_repo.apply(resource_id, metadata, activity_execution_id)
    return result
```

Notes:

* activity workers may execute the same logical activity more than once
* durable deduplication is mandatory
* `activity_execution_id` identifies the logical side effect across retries
* `attempt_no` identifies one delivery attempt and is used by Fluxi to fence stale late completions

## 8. Deployment Shape

Planned deployment roles:

* `gateway`
* `order-api`
* `payment-api`
* `stock-api`
* `fluxi-server` replicas
* `order-workflow-worker` replicas
* `payment-activity-worker` replicas
* `stock-activity-worker` replicas
* Redis master
* Redis replica(s)
* Redis Sentinel instances

This keeps the public API stable while letting worker roles scale independently from HTTP traffic.

## 9. Evaluation Checklist

* **Stable public API:** preserved
* **Coordination extracted into an orchestrator:** yes
* **Temporal-like workflow execution:** yes, via workflow workers plus replay from durable history
* **Async internal communication:** yes, via Redis Streams
* **Crash recovery:** yes, with replay, retries, and idempotent activities
* **Exactly-once execution:** no, not assumed
* **Delivery model:** at-least-once
* **Worker scaling:** yes, multiple replicas per worker type
* **Redis Cluster:** intentionally not used in v1

## 10. Open Risks

* Redis failover can still lose recent writes.
* Sentinel-based HA in Docker Compose is still weaker than true multi-machine HA.
* Workflow determinism rules must be enforced carefully or replay will diverge.
* Idempotency must be implemented correctly in every activity or retries become unsafe.
* Load testing may show that the single Redis master becomes a bottleneck at higher throughput. If that happens, cluster partitioning or a different state/broker split can be considered later.
