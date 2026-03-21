# Requirements

### **1\. Functional Requirements**

**1.1. Orchestrator Engine (The Coordination Plane)**

* **Workflow State Management:** The system must durably record and track the state of all active, completed, and failed workflows.  
* **Logical Task Queuing:** The system must maintain distinct logical queues for different microservices or worker types.  
* **Task Dispatching:** The system must assign pending activities to available workers listening on the corresponding logical queues.  
* **State Machine Advancement:** Upon receiving a completion or failure signal from a worker, the orchestrator must determine the next step in the workflow and queue the subsequent tasks.  
* **Timeout & Retry Handling:** The system must monitor dispatched tasks. If a task is not acknowledged within a configurable timeout period, the system must mark it as failed or re-queue it for another worker.

**1.2. Client SDK & Workers (The Execution Plane)**

* **Declarative Registration:** The SDK must provide a mechanism for developers to explicitly register local functions as either "Workflows" or "Activities".  
* **Execution Interception:** When an activity is invoked from within a workflow, the SDK must intercept the invocation, prevent local execution, and forward the execution request to the Orchestrator Engine.  
* **Workflow Suspension/Resumption:** The SDK must allow a workflow's execution to pause while waiting for remote activities to complete, and resume from the exact point of suspension once the result is returned.  
* **Background Task Polling:** The SDK must include a background process that continuously requests pending tasks from the Orchestrator Engine for a specified logical queue.  
* **Result Reporting:** Once a worker finishes executing a task's business logic, it must report the success (with return values) or failure (with error details) back to the Orchestrator Engine.

### **2\. Non-Functional Requirements**

**2.1. Fault Tolerance & Resilience**

* **Worker Crash Recovery (The "Kill Container" Test):** If a worker instance fails or is terminated while processing a task, the system must detect the failure and reassign the task to a healthy worker instance without data loss.  
* **Orchestrator Crash Recovery:** If the Orchestrator Engine fails or is terminated mid-execution, upon restart, it must accurately reconstruct the state of all in-flight workflows and resume orchestration seamlessly.  
* **Idempotency Guarantees:** The system must ensure that tasks re-queued due to timeouts or crashes do not cause inconsistent states if executed multiple times by the workers.

**2.2. Scalability & Performance**

* **Horizontal Scalability of Workers:** The system must support adding multiple instances of the same worker type.  
* **Competing Consumers Load Balancing:** Tasks within a logical queue must be dynamically and evenly distributed among all available worker instances listening to that queue.  
* **Horizontal Scalability of Orchestrator:** The Orchestrator Engine must be deployable as multiple stateless instances to handle high-throughput workflow transitions.  
* **Asynchronous Communication:** The overall architecture must be event-driven and non-blocking, ensuring that system throughput is not constrained by synchronous network waits between the orchestrator and the workers.

**2.3. Developer Experience & Maintainability**

* **Separation of Concerns:** Business logic (microservices) must be strictly decoupled from distributed coordination logic. Microservices must not communicate directly with one another for transaction coordination.  
* **"Workflows as Code":** Developers must be able to define the coordination of distributed transactions using standard programming constructs (e.g., standard error handling, loops, and conditional statements) rather than complex external configuration files or explicit graph definitions.

# Architecture Overview

## **1\. Executive Summary**

For Phase 2 of the Distributed Data Systems project, we have completely abstracted distributed transaction coordination (SAGAs and 2PC) away from the business microservices. Instead of microservices calling each other synchronously via HTTP, we implemented a **"Workflows as Code"** orchestrator inspired by **Temporal.io**.

Our system is a fully asynchronous, event-driven architecture that guarantees consistency, survives arbitrary container failures (the "kill container" test), and scales horizontally. The business microservices (Order, Stock, Payment) have been reduced to "dumb" workers that simply execute tasks and poll a centralized message broker.

## **2\. System Architecture (The Three Layers)**

The system is divided into three distinct layers, deployed via docker-compose.

### **Layer 1: The State & Routing Engine (Redis)**

Instead of relying on in-memory state, **Redis** acts as the durable brain of the entire cluster.

* **State Store (Key-Value):** Stores the current state, history, and variables of every in-flight workflow (e.g., {order\_123}:state).  
* **Task Queues (Redis Streams):** Acts as the message broker. We maintain logical queues (e.g., queue:stock, queue:payment) using Redis Streams.

### **Layer 2: The Orchestrator Server (The "Temporal Server")**

A standalone, stateless Python application (built with Quart/FastAPI) that manages the state machine.

* **Frontend API:** Receives HTTP requests from the microservices to start workflows (e.g., StartWorkflow("CheckoutSaga", order\_id)).  
* **State Machine Executor:** When notified that an activity has completed, it reads the workflow state from Redis, calculates the next step (or compensation), writes the updated state to Redis, and pushes the next task to the respective Redis Stream.  
* **Stateless & Scalable:** Because it holds zero state in memory, we can spin up multiple Orchestrator replicas behind a load balancer for zero-downtime high availability.

### **Layer 3: The Microservices & SDK (The "Workers")**

The Order, Stock, and Payment services contain the actual business logic. They import our custom **Client SDK**.

* **Worker Daemon:** An asyncio background loop that boots up with the microservice. It uses the Competing Consumers pattern (XREADGROUP) to long-poll its specific Redis Stream for tasks.  
* **RPC Interception:** The SDK provides @workflow and @activity decorators. When a workflow calls an activity, the SDK intercepts it, suspends local execution, and sends a task to the Orchestrator Server.

## **3\. Architectural Mapping: Mini-Temporal vs. Real Temporal**

To achieve Temporal's enterprise-grade patterns within our constraints, we mapped Temporal's internal Go-based services directly to high-performance Redis primitives:

| Real Temporal Component | Our Implementation (Mini-Temporal) | Purpose / Benefit |
| :---- | :---- | :---- |
| **Frontend Service** | **Orchestrator HTTP API** | Ingests requests to start/signal workflows and decouples web traffic from execution. |
| **History Service (Shards)** | **Stateless Orchestrator \+ Redis Hash Tags** | Using {workflow\_id}:state ensures all data for a workflow routes to the same Redis CPU cluster shard, preventing bottlenecks. |
| **Matching Service (Partitions)** | **Redis Streams \+ Consumer Groups** | Automatically load-balances tasks across multiple containers natively. Replaces complex partition-tree routing. |
| **Worker Poller Threads** | **SDK asyncio Background Loop** | Pull-based task execution. Workers only fetch tasks when they have capacity. |

## **4\. Deep Dive: Core Mechanisms**

### **A. Event-Driven Routing (Pull, Not Push)**

The Orchestrator does *not* push HTTP requests to the Stock or Payment services.

1. The Orchestrator pushes a JSON payload to the queue:stock Redis Stream.  
2. The Stock Service workers use XREADGROUP (Blocking Read) to poll the stream.  
3. This completely decouples the services. If the Stock service is overwhelmed, tasks queue safely in Redis until a worker is free.

### **B. Fault Tolerance (Surviving docker kill)**

To survive the lecturer's "kill container" benchmark without losing money or items:

* **The PEL (Pending Entries List):** When a worker pulls a task via XREADGROUP, Redis moves the task to the PEL. It is *not* deleted.  
* **Acknowledgment (XACK):** The task is only deleted when the worker finishes the DB transaction and sends an XACK to Redis.  
* **Recovery (XCLAIM):** If a container is killed mid-task, the XACK is never sent. A background watchdog in the Orchestrator detects tasks pending for \>5 seconds and uses XCLAIM to immediately reassign the task to a surviving Worker container. *Zero data loss.*

### **C. Concurrency Control (Race Conditions & 2PC)**

In a **Two-Phase Commit (2PC)** or parallel SAGA, multiple workers (e.g., Stock and Payment) might reply "READY" at the exact same millisecond.

* If two stateless Orchestrator replicas receive these events simultaneously, they will race to update the workflow state.  
* **Solution:** We use **Optimistic Locking (Redis WATCH / MULTI / EXEC)**. Before advancing the state machine, the Orchestrator watches the state key. If another replica modified it during the calculation, the transaction aborts and the Orchestrator retries. This guarantees perfectly sequential state transitions.		

## **5\. Developer Experience: The Client SDK**

The ultimate goal of Phase 2 is abstraction. By importing our SDK, the business logic inside the microservices is entirely freed from coordination boilerplate.

**Defining an Activity (Stock Service):**

\`\`\`  
from temporal\_sdk import activity, worker

@activity.defn(queue="queue:stock")  
async def deduct\_stock(item\_id: str, amount: int):  
    \# Pure DB logic. No network coordination required.  
    db.execute("UPDATE stock SET amount \= amount \- ? WHERE id \= ?", amount, item\_id)  
    return {"status": "success"}

\# Starts the background puller  
worker.start(queue="queue:stock", activities=\[deduct\_stock\])  
\`\`\`

**Defining a Workflow (Order Service):**  
Instead of complex DAGs or HTTP callbacks, SAGA logic is written as standard, synchronous-looking Python code using try/finally blocks and await.

\`\`\`  
from temporal\_sdk import workflow  
from stubs import deduct\_stock, process\_payment \# Type stubs

@workflow.defn(queue="queue:order")  
class CheckoutSaga:  
      
    @workflow.run  
    async def run(self, order\_id: str, user\_id: str, total\_cost: int, items: dict):  
        compensations \=\[\]  
        try:  
            \# 1\. Process Payment (Blocks until Orchestrator routes and Worker finishes)  
            await workflow.execute\_activity(process\_payment, args=\[user\_id, total\_cost\])  
            compensations.append(refund\_payment)  
              
            \# 2\. Deduct Stock  
            await workflow.execute\_activity(deduct\_stock, args=\[items\])  
            compensations.append(add\_stock)  
              
            return "SUCCESS"  
              
        except Exception as e:  
            \# Automatic SAGA Rollback  
            for comp in reversed(compensations):  
                await workflow.execute\_activity(comp, args=\[...\])  
            return "FAILURE"  
\`\`\`

## **6\. Evaluation Criteria Checklist**

* ✅ **Consistency:** Distributed SAGA/2PC implementation \+ Optimistic Locking.  
* ✅ **Performance (10k RPS target):** Fully asynchronous I/O (Quart/asyncio), Redis pipelining, and load-balanced workers via Consumer Groups.  
* ✅ **Architecture Difficulty:** Implemented an RPC-intercepting SDK, event-sourcing, and a custom decoupled Matching Service.  
* ✅ **Fault Tolerance:** XREADGROUP \+ PEL \+ XCLAIM guarantees exactly-once (or at-least-once with idempotent workers) execution even if instances are violently killed.