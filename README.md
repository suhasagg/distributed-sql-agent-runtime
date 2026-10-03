# Distributed SQL Agent Runtime
## Durable, Transactional, Multi-Agent Execution Platform

The project demonstrates how to combine **LangGraph**, **FastAPI**, **CockroachDB-compatible distributed SQL**, **Redis**, **Kafka/Redpanda**, **S3/MinIO**, and **OpenTelemetry-oriented observability patterns** to solve the hard problems that appear when AI agents move from demos to enterprise systems:

- workflows lasting seconds, hours, days, or weeks;
- process crashes and resumability;
- multi-agent concurrency;
- durable checkpoints;
- human-in-the-loop approvals;
- idempotent external actions;
- ambiguous tool outcomes;
- transactional event publication;
- auditability;
- horizontal worker scaling;
- multi-region deployment;
- schema/workflow versioning;
- security boundaries between probabilistic models and deterministic execution.

> **Core principle:** the LLM is a reasoning component, not the workflow database. Durable SQL owns committed execution truth.

---

# 1. What problem does this project solve?

A prototype agent can often keep its state in memory:

```text
prompt -> LLM -> tool -> LLM -> answer
```

That architecture becomes unsafe when an agent performs real work.

Consider:

```text
"Analyze Q3 sales, generate a presentation,
send it to leadership, and schedule a review."
```

The workflow may become:

```text
RUN 93812
 |
 +-- T1 Fetch sales data        SUCCEEDED
 +-- T2 Analyze data            SUCCEEDED
 +-- T3 Generate report         SUCCEEDED
 +-- T4 Create presentation     SUCCEEDED
 +-- T5 Send email              WAITING_APPROVAL
 +-- T6 Schedule meeting        PENDING
```

Now assume the worker dies after T4.

A robust platform must answer:

- What run was executing?
- Which tasks committed successfully?
- What was the latest checkpoint?
- Was the email already sent?
- Is an approval outstanding?
- Which worker owns a task?
- Has its lease expired?
- Can another worker safely continue?
- Is a retry safe?
- What if an external API succeeded but its response was lost?
- Which graph/software version created this state?
- What should happen during a regional outage?

Those are distributed-systems problems. This repository demonstrates a design in which distributed SQL is the durable execution authority.

---

# 2. Architecture

```text
                    Users / Applications
                            |
                            v
                 +-----------------------+
                 | Agent API / Gateway   |
                 | FastAPI               |
                 +-----------+-----------+
                             |
                             v
                 +-----------------------+
                 | Agent Orchestration   |
                 | LangGraph             |
                 | Planner / Supervisor  |
                 +-----------+-----------+
                             |
                             v
+-------------------------------------------------------------------+
|                    Durable Agent Runtime                           |
|                                                                   |
|  +--------------------+     +----------------------------------+   |
|  | Distributed SQL    |     | Redis                            |   |
|  | CockroachDB        |     | ephemeral cache / hints          |   |
|  |                    |     +----------------------------------+   |
|  | runs               |                                            |
|  | tasks              |     +----------------------------------+   |
|  | attempts           |     | Kafka / Redpanda                 |   |
|  | checkpoints        |     | async events / commands          |   |
|  | approvals          |     +----------------------------------+   |
|  | tool calls         |                                            |
|  | audit              |     +----------------------------------+   |
|  | outbox             |     | S3 / MinIO                      |   |
|  +--------------------+     | artifacts / large snapshots      |   |
|                             +----------------------------------+   |
|                                                                   |
|                             OpenTelemetry                          |
|                             traces / metrics / logs                |
+------------------------------+------------------------------------+
                               |
                               v
                    +-----------------------+
                    | Tool / MCP Gateway    |
                    | policy + validation   |
                    +-----------+-----------+
                                |
                                v
                     Enterprise Systems/APIs
```

## Store responsibilities

| Concern | Store |
|---|---|
| workflow truth | Distributed SQL |
| task state | Distributed SQL |
| checkpoints/metadata | Distributed SQL |
| approvals | Distributed SQL |
| idempotency ledger | Distributed SQL |
| tool-call status | Distributed SQL |
| audit metadata | Distributed SQL |
| event outbox | Distributed SQL |
| ephemeral cache | Redis |
| asynchronous distribution | Kafka/Pulsar |
| semantic retrieval/RAG | Vector DB |
| entity relationships | Graph DB |
| large documents/screenshots | S3/blob storage |
| traces/metrics | OpenTelemetry backend |

Distributed SQL does **not** replace every other datastore. It owns the state requiring transactional consistency.

---

# 3. Major applications

## 3.1 Durable autonomous workflows

Agents may wait on people, APIs, batch jobs, or external events. Persisting execution state allows a replacement worker to resume rather than reconstructing history from an LLM conversation.

Typical applications:

- enterprise copilots;
- autonomous SRE agents;
- software-engineering agents;
- research agents;
- finance operations;
- procurement workflows;
- CRM automation;
- customer-support orchestration;
- browser/computer-use agents;
- document processing;
- approval-heavy enterprise automation.

## 3.2 Distributed checkpointing

Each durable checkpoint can contain:

```text
run_id
thread_id
graph_version
checkpoint_version
node
state
created_at
```

Checkpoints allow execution to restart from a known committed state.

Small state can live in SQL JSON. Large state should normally be stored in object storage with SQL containing an immutable reference and hash.

## 3.3 Multi-agent coordination

Example:

```text
                    Supervisor
                        |
       +----------------+----------------+
       |                |                |
       v                v                v
 Research Agent     Data Agent      Coding Agent
       |                |                |
       +----------------+----------------+
                        |
                        v
                  Shared workflow
```

Multiple workers can race to modify the same workflow. Database transactions, row locks, versions, leases, and invariants protect shared state.

## 3.4 Human approval

High-risk actions should not be authorized merely because an LLM generated them.

```text
Model proposes action
        |
        v
Validate schema
        |
        v
Authorization/policy
        |
        v
Risk classification
        |
        +------> Human approval
        |
        v
Idempotency reservation
        |
        v
Execute tool
        |
        v
Audit result
```

Approval is bound to an `action_hash`. If action parameters change, the approval no longer represents the same operation.

## 3.5 Financial or otherwise sensitive external actions

Example:

```text
Agent -> send payment -> provider executes payment
                         |
                         X response lost
```

A naive retry can duplicate the action.

The runtime records durable intent:

```text
STARTED -> SUCCEEDED
        -> FAILED
        -> UNKNOWN
```

`UNKNOWN` means: **do not blindly retry; reconcile with the provider**.

## 3.6 Global agent platforms

Distributed SQL is useful when execution spans:

```text
US        Europe        APAC
 |           |            |
 +-----------+------------+
             |
      Distributed SQL
```

Possible benefits include regional execution, survivability, tenant-local state, and residency-aware architecture. Strong cross-region transactions still have physical latency costs, so locality design matters.

---

# 4. Repository structure

```text
distributed-sql-agent-runtime/
|
+-- app/
|   +-- api/
|   |   +-- routes.py
|   |
|   +-- core/
|   |   +-- config.py
|   |
|   +-- db/
|   |   +-- base.py
|   |   +-- models.py
|   |   +-- session.py
|   |
|   +-- domain/
|   |   +-- state.py
|   |
|   +-- runtime/
|   |   +-- graph.py
|   |
|   +-- services/
|   |   +-- runtime_service.py
|   |
|   +-- tools/
|   |   +-- gateway.py
|   |
|   +-- workers/
|   |   +-- task_worker.py
|   |   +-- outbox_worker.py
|   |
|   +-- main.py
|
+-- docs/
|   +-- PRODUCTION_DESIGN.md
|
+-- tests/
|   +-- test_hash.py
|   +-- test_state_machine.py
|
+-- docker-compose.yml
+-- Dockerfile
+-- Makefile
+-- pyproject.toml
+-- .env.example
+-- README.md
```

---

# 5. Code walkthrough

## `app/main.py`

Creates the FastAPI application, registers API routes, exposes health checking, and initializes tables for local demonstration.

The automatic `create_all()` behavior is intentionally a development convenience. Production deployments should use controlled migrations such as Alembic.

## `app/core/config.py`

Uses `pydantic-settings` to load:

- database URL;
- Redis URL;
- Kafka brokers/topic;
- S3 endpoint and credentials;
- environment;
- worker lease duration.

Production systems should retrieve secrets from a managed secrets service rather than source-controlled environment files.

## `app/db/base.py`

Defines the SQLAlchemy declarative base used by all relational entities.

## `app/db/session.py`

Creates the asynchronous SQLAlchemy engine and session factory.

This is the persistence boundary used by the API and workers.

## `app/db/models.py`

Contains the authoritative runtime schema.

### `AgentRun`

Represents one end-to-end agent execution.

Important fields:

```text
id
tenant_id
objective
status
graph_version
version
created_at
updated_at
```

`graph_version` matters because long-running executions can survive software deployments.

### `Task`

Represents a durable unit of work.

```text
run_id
name
status
payload
result
version
lease_owner
lease_expires_at
```

`version` supports optimistic concurrency.

`lease_owner` and `lease_expires_at` support distributed worker claiming.

### `TaskAttempt`

Represents individual execution attempts. In a mature platform, attempts are useful for retries, debugging, metrics, and postmortems.

### `Checkpoint`

Stores durable graph execution state:

```text
run_id
thread_id
graph_version
checkpoint_version
node
state
```

A uniqueness constraint prevents duplicate versions for a run.

### `Approval`

Stores human authorization state:

```text
run_id
task_id
principal_id
action_hash
nonce
status
expires_at
decided_by
decided_at
```

The action hash protects against approving one payload and executing another.

### `ToolCall`

The external-effect ledger:

```text
tool_name
idempotency_key
request_hash
status
provider_ref
response
```

The unique idempotency key prevents the runtime from creating multiple logical executions for the same action.

### `AuditEvent`

Append-oriented audit metadata for security, compliance, debugging, and incident analysis.

### `OutboxEvent`

Implements the transactional-outbox pattern. Business state and the event describing that state are committed in the same database transaction.

---

# 6. State machine

The domain layer defines explicit task states.

```text
PENDING
   |
   v
 READY
   |
   v
RUNNING
 |  |  |  \
 |  |  |   +------> UNKNOWN
 |  |  +----------> FAILED
 |  +-------------> WAITING_APPROVAL
 +----------------> SUCCEEDED
```

Other supported transitions include cancellation and controlled retry.

The key concept is that arbitrary state mutation is not permitted. `assert_task_transition()` checks transitions against a deterministic transition table.

Why?

Without a state machine, race conditions can create nonsense such as:

```text
SUCCEEDED -> RUNNING
```

or two incompatible terminal states.

---

# 7. Runtime service

`app/services/runtime_service.py` is the main transactional domain service.

## Creating a run

`create_run()`:

1. creates the run;
2. creates initial tasks;
3. writes an outbox event;
4. writes an audit event;
5. commits them transactionally.

This demonstrates an important rule:

> state mutation and the event announcing that mutation should not be two unrelated commits.

## Claiming tasks

Workers claim `READY` work using:

```sql
SELECT ...
FOR UPDATE SKIP LOCKED
```

Conceptually:

```text
Worker A ----\
Worker B -----+--> READY tasks
Worker C ----/
```

Each worker locks a candidate row and skips rows already locked by another claimant.

The task receives:

```text
status = RUNNING
lease_owner = worker-X
lease_expires_at = timestamp
```

The lease allows abandoned work eventually to become reclaimable.

## Optimistic concurrency

Task transitions include an expected version:

```text
read version 8
       |
       v
update WHERE version = 8
       |
       +-- success -> version 9
       |
       +-- no row -> concurrency conflict
```

This prevents stale workers from silently overwriting newer state.

## Checkpoints

`checkpoint()` persists graph state with a monotonically meaningful checkpoint version.

## Approval requests

`request_approval()` calculates a canonical action hash, generates a nonce, establishes expiration, and publishes an approval-requested outbox event.

## Approval decisions

The approval row is locked before resolution.

Only `PROPOSED` approvals can be decided. Expired approvals become `EXPIRED`.

## Tool-call reservation

Before an external action:

```text
reserve idempotency key
        |
        +-- existing -> return existing state
        |
        +-- new -> create STARTED record
```

This is central to safe retries.

---

# 8. Tool gateway

`app/tools/gateway.py` demonstrates a controlled external HTTP action.

The execution sequence is:

```text
1. reserve durable tool call
2. commit STARTED
3. invoke provider
4. record outcome
```

Provider idempotency is propagated using:

```http
Idempotency-Key: <key>
```

when the downstream provider supports that semantic.

### Success

```text
STARTED -> SUCCEEDED
```

### Known failure

```text
STARTED -> FAILED
```

### Ambiguous timeout

```text
STARTED -> UNKNOWN
```

This distinction is essential.

A timeout tells you that the client did not receive a response. It does **not** prove that the server did not execute the action.

---

# 9. Why exactly-once is not assumed

Suppose database D and provider P are independent systems.

You cannot atomically commit:

```text
D: "payment executed"
P: execute payment
```

without a shared distributed transaction protocol, which ordinary SaaS APIs do not provide.

Therefore practical agent platforms use:

- at-least-once delivery;
- idempotency;
- provider-native request keys;
- durable intent;
- reconciliation;
- deduplication;
- deterministic state transitions.

The runtime makes uncertainty explicit rather than pretending it does not exist.

---

# 10. Transactional outbox

The classic failure:

```text
commit DB state
      |
      X process crashes
      |
publish Kafka event never happens
```

Instead:

```text
BEGIN

update task
insert outbox_event

COMMIT
```

A separate publisher later reads unpublished outbox rows:

```text
SQL outbox -> Kafka -> consumers
```

If the publisher crashes, the row remains unpublished and can be retried.

Consumers should still be idempotent because duplicate publication is possible around failures.

---

# 11. Worker model

`app/workers/task_worker.py` implements the basic worker lifecycle:

```text
poll
 |
 v
claim READY task
 |
 v
RUNNING + lease
 |
 v
execute
 |
 +--> SUCCEEDED
 |
 +--> FAILED
```

A production worker should additionally support:

- lease renewal;
- fencing tokens for sensitive resources;
- retry policy by error class;
- cancellation;
- graceful shutdown;
- deadlines;
- model/tool rate limits;
- circuit breakers;
- bulkheads;
- poison-task quarantine;
- telemetry;
- reconciliation.

---

# 12. Fencing and stale workers

Leases alone do not completely solve stale-worker problems.

Example:

```text
Worker A gets lease
Worker A pauses
lease expires
Worker B gets new lease
Worker A wakes up
```

Both may now believe they can act.

For high-risk resources, use a monotonically increasing **fencing token**:

```text
Worker A token = 41
Worker B token = 42
```

A downstream component capable of fencing rejects operations carrying stale token 41.

---

# 13. LangGraph layer

`app/runtime/graph.py` demonstrates:

```text
Planner
   |
   v
Supervisor
   |
   v
Finalize
```

LangGraph is responsible for graph semantics and routing.

Distributed SQL is responsible for durable enterprise execution truth.

These roles are complementary:

```text
LangGraph = what should execute next?
SQL       = what has durably happened?
```

A mature implementation can connect LangGraph checkpoints directly to the durable checkpoint service and maintain graph-version compatibility.

---

# 14. Memory architecture

"Agent memory" should not be treated as one database.

A robust design separates layers:

```text
                     Agent Memory
                          |
       +------------------+-------------------+
       |                  |                   |
       v                  v                   v
 Transactional        Semantic            Ephemeral
 memory/state         memory              memory
       |                  |                   |
 Distributed SQL      Vector DB              Redis
```

Additional relationship-heavy knowledge may live in a graph database.

### Distributed SQL

Good for:

- workflow state;
- task state;
- approvals;
- identity metadata;
- checkpoint metadata;
- durable preferences requiring transactions;
- tool history;
- audit metadata.

### Vector database

Good for:

- semantic retrieval;
- RAG chunks;
- episodic similarity search;
- embedding-based memory.

### Redis

Good for:

- caches;
- rate-limit counters;
- temporary session data;
- reconstructible coordination hints.

### Graph database

Good for:

- entities;
- relationships;
- dependency networks;
- knowledge graphs.

---

# 15. Human-in-the-loop architecture

Example sensitive action:

```text
Agent wants to submit $25,000 invoice
              |
              v
       Policy engine
              |
              v
      approval required
              |
              v
       WAITING_APPROVAL
              |
       +------+------+
       |             |
    approve        reject
       |             |
       v             v
     READY        CANCELLED
       |
       v
 tool execution
```

Approval records should capture:

```text
who requested
who approved
exact action hash
expiration
nonce
run/task
decision time
```

Authentication and authorization of the approver must be enforced outside the LLM.

---

# 16. Security model

Treat all model output as **untrusted proposed input**.

Recommended boundary:

```text
LLM
 |
 v
structured output parser
 |
 v
schema validation
 |
 v
authorization
 |
 v
policy engine
 |
 v
risk classification
 |
 +--> approval
 |
 v
tool gateway
 |
 v
enterprise system
```

Important controls:

- OIDC/OAuth/JWT;
- service identities;
- tenant isolation;
- least privilege;
- tool allowlists;
- schema validation;
- egress allowlists;
- secrets isolation;
- KMS encryption;
- prompt-injection defenses;
- per-tool risk classes;
- approval policies;
- immutable/action-bound approval;
- audit logging;
- payload limits;
- data retention;
- PII/secret redaction;
- rate limiting.

The model should never possess raw infrastructure credentials simply because it may need to call a tool.

---

# 17. Multi-tenancy

Every enterprise record should be associated with a tenant/security domain.

This reference already carries `tenant_id` at the run/audit level, but a full production implementation should enforce tenancy consistently at every data-access boundary.

Typical model:

```text
tenant
 |
 +-- users
 +-- agents
 +-- runs
 +-- tasks
 +-- approvals
 +-- tool policies
 +-- memory
 +-- artifacts
```

Do not rely solely on application convention. Defense-in-depth can include database policies, separate credentials, encryption domains, or physically separated deployments for high-isolation tenants.

---

# 18. Multi-region design

Possible topology:

```text
                   Global Gateway
                        |
        +---------------+---------------+
        |               |               |
        v               v               v
      US API           EU API         APAC API
        |               |               |
      workers          workers         workers
        |               |               |
        +---------------+---------------+
                        |
                 CockroachDB
```

Important design questions:

- What is each tenant's home region?
- Where should workflow rows live?
- Which data must remain in-region?
- Which operations need global consistency?
- What is the tolerated regional RPO/RTO?
- What happens to an in-flight tool call during region loss?
- Can execution move regions without violating residency?
- What latency does a cross-region transaction add?

Distributed SQL removes some operational complexity, but not the laws of physics.

---

# 19. CockroachDB considerations

CockroachDB uses serializable transaction semantics. Under contention, transactions can require retries.

Production repository code should therefore add bounded retry handling for retryable transaction failures.

Avoid hot global rows such as:

```text
global_counter
global_scheduler_lock
single mutable configuration row
```

Prefer partitionable access patterns and UUID identifiers.

Other considerations:

- TLS certificates;
- connection pooling;
- locality configuration;
- regional survival policy;
- schema-change procedures;
- backup/restore;
- observability;
- contention dashboards;
- transaction retry metrics;
- realistic cross-region load testing.

---

# 20. Observability

Every trace/log/metric should carry useful correlation dimensions when available:

```text
tenant_id
run_id
task_id
attempt_id
tool_call_id
approval_id
worker_id
graph_version
region
model
provider
trace_id
```

Important metrics:

```text
run success rate
run failure rate
UNKNOWN rate
task latency
queue latency
claim latency
approval wait time
tool latency
tool failure rate
reconciliation backlog
DB transaction retry rate
checkpoint latency
checkpoint size
model token usage
model cost
worker saturation
Kafka lag
```

Useful traces:

```text
HTTP request
  -> create run transaction
  -> worker claim
  -> model call
  -> checkpoint
  -> approval
  -> tool call
  -> provider request
  -> result transaction
  -> outbox publish
```

---

# 21. Data model relationships

```text
AgentRun
  |
  +----< Task
  |       |
  |       +----< TaskAttempt
  |       |
  |       +----< Approval
  |       |
  |       +----< ToolCall
  |
  +----< Checkpoint
  |
  +----< AuditEvent

OutboxEvent
  |
  +---- asynchronous representation of committed changes
```

---

# 22. Local prerequisites

Recommended:

- Docker Engine 24+;
- Docker Compose v2;
- 4+ CPU cores;
- 8 GB+ RAM available to Docker;
- Git;
- curl;
- optional Python 3.11/3.12 for running tests outside containers.

Verify:

```bash
docker --version
docker compose version
python3 --version
```

---

# 23. Quick start with Docker Compose

## Step 1 — extract the repository

```bash
unzip distributed-sql-agent-runtime.zip
cd distributed-sql-agent-runtime
```

## Step 2 — create environment configuration

```bash
cp .env.example .env
```

Local defaults include:

```text
CockroachDB :26257
Cockroach UI :8080
Redis       :6379
Kafka       :9092
MinIO       :9000
MinIO UI    :9001
API         :8000
```

## Step 3 — build and start

```bash
docker compose up --build
```

Or detached:

```bash
docker compose up --build -d
```

## Step 4 — inspect services

```bash
docker compose ps
```

## Step 5 — health check

```bash
curl http://localhost:8000/health
```

Expected:

```json
{"status":"ok"}
```

## Step 6 — create a run

```bash
curl -X POST http://localhost:8000/v1/runs \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id":"acme",
    "objective":"Analyze Q3 sales, create a presentation, send it to leadership and schedule a review."
  }'
```

Example response:

```json
{
  "run_id":"<uuid>",
  "status":"RUNNING"
}
```

---

# 24. Start workers

The Compose file starts the API infrastructure but the worker programs can be launched separately for clarity.

From the project container/environment:

```bash
python -m app.workers.task_worker
```

Start the outbox publisher:

```bash
python -m app.workers.outbox_worker
```

For multiple task workers:

```bash
WORKER_ID=worker-1 python -m app.workers.task_worker
WORKER_ID=worker-2 python -m app.workers.task_worker
WORKER_ID=worker-3 python -m app.workers.task_worker
```

Each worker competes for `READY` tasks using database locking and leases.

---

# 25. Running without Docker for the Python application

Infrastructure can remain in Docker while the API runs locally.

Create a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install:

```bash
pip install -e ".[dev]"
```

Adjust `.env` hostnames from Compose service names to localhost where necessary, for example:

```text
DATABASE_URL=postgresql+asyncpg://root@localhost:26257/agent_runtime?sslmode=disable
REDIS_URL=redis://localhost:6379/0
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
S3_ENDPOINT_URL=http://localhost:9000
```

Start:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

---

# 26. Testing

Run:

```bash
make test
```

or:

```bash
pytest -q
```

Current tests validate core invariants such as:

- legal state transitions;
- illegal terminal transitions;
- deterministic canonical hashing.

A production test suite should additionally cover:

- concurrent task claiming;
- stale versions;
- expired leases;
- duplicate idempotency keys;
- approval races;
- expired approvals;
- timeout -> UNKNOWN;
- reconciliation;
- duplicate outbox delivery;
- DB transaction retries;
- process crash injection;
- Kafka outage;
- provider outage;
- regional failure;
- graph-version upgrades.

---

# 27. Linting

```bash
make lint
```

or:

```bash
ruff check .
```

---

# 28. Useful local interfaces

When using the supplied Compose configuration:

```text
API:
http://localhost:8000

FastAPI OpenAPI:
http://localhost:8000/docs

CockroachDB Admin UI:
http://localhost:8080

MinIO API:
http://localhost:9000

MinIO Console:
http://localhost:9001
```

These are local development endpoints and should not be exposed publicly with default configuration.

---

# 29. Database inspection

Open Cockroach SQL:

```bash
docker compose exec cockroach \
  cockroach sql --insecure --host=localhost:26257
```

Useful commands:

```sql
SHOW DATABASES;
USE agent_runtime;
SHOW TABLES;

SELECT * FROM agent_runs;
SELECT * FROM tasks;
SELECT * FROM checkpoints;
SELECT * FROM approvals;
SELECT * FROM tool_calls;
SELECT * FROM audit_events;
SELECT * FROM outbox_events;
```

---

# 30. Failure scenario walkthrough

Assume a worker owns task T4.

```text
T4 READY
 |
 v
Worker A claims
 |
 v
T4 RUNNING
lease_owner=A
 |
 v
Worker A crashes
```

The durable database still contains the task.

After lease expiry:

```text
Worker B
 |
 v
claims recoverable task
 |
 v
continues execution
```

For a pure computation this may simply be retried.

For an external side effect, the worker must first inspect the tool-call ledger and provider state.

---

# 31. External-action failure walkthrough

```text
Tool call reserved
status=STARTED
      |
      v
provider request
      |
      v
provider executes
      |
      X
response lost
```

Runtime records:

```text
status=UNKNOWN
```

A reconciliation process should use:

```text
provider reference
idempotency key
business identifier
request hash
```

to query provider state.

Possible resolution:

```text
UNKNOWN -> SUCCEEDED
UNKNOWN -> FAILED
```

Only after establishing provider state should the workflow continue or retry.

---

# 32. Approval TOCTOU protection

TOCTOU means **time-of-check to time-of-use**.

Unsafe flow:

```text
approve payment amount=100
later execute amount=10,000
```

The repository's action-hash concept binds approval to exact action content:

```text
hash(canonical action payload)
```

Execution should recompute and compare the hash immediately before the privileged action.

Changed payload = new approval.

---

# 33. Redis usage

Redis is intentionally not the source of truth.

Appropriate future additions include:

```text
run summary cache
rate limiting
short-lived locks
presence
worker heartbeat hints
model-response cache
tool metadata cache
```

If Redis is flushed, the platform should reconstruct correctness from durable state.

---

# 34. Kafka/Redpanda usage

Kafka is useful for:

```text
task notifications
workflow events
analytics
audit pipelines
downstream integrations
reconciliation commands
notifications
```

The SQL outbox bridges transactional state to asynchronous messaging.

Consumer handlers should be idempotent.

---

# 35. S3/MinIO usage

Do not put arbitrarily large content into transactional rows.

Store large data such as:

```text
PDF reports
presentations
screenshots
browser recordings
large checkpoint blobs
generated archives
documents
model artifacts
```

in object storage.

SQL stores:

```text
artifact_id
URI/object key
content hash
media type
size
tenant
encryption metadata
creation time
```

This keeps transactional rows manageable.

---

# 36. Schema and workflow versioning

Agent workflows evolve.

A run created by:

```text
graph_version=v17
```

may still be alive when production deploys:

```text
v18
v19
```

Strategies:

1. keep old graph implementations executable;
2. implement explicit state migrations;
3. drain old workflows before incompatible deployment;
4. version tool contracts;
5. version checkpoint schemas.

Never assume every active workflow was created by the current binary.

---

# 37. Retry strategy

Not every failure should retry.

Suggested classification:

```text
TRANSIENT
  timeout before known side effect
  temporary DB contention
  429
  temporary 503

PERMANENT
  invalid schema
  authorization failure
  unsupported operation

AMBIGUOUS
  timeout after external request sent
  lost provider response
```

Policies:

```text
TRANSIENT -> bounded exponential backoff + jitter
PERMANENT -> fail
AMBIGUOUS -> UNKNOWN + reconcile
```

---

# 38. Production deployment model

A representative deployment:

```text
                    Load Balancer
                         |
               +---------+---------+
               |                   |
             API-1               API-N
               |                   |
               +---------+---------+
                         |
                  CockroachDB
                         |
        +----------------+----------------+
        |                |                |
     Worker-1         Worker-2         Worker-N
        |
   Tool Gateway
        |
 enterprise APIs

SQL Outbox -> Publisher Fleet -> Kafka
Artifacts ---------------------> S3
Telemetry ---------------------> OTel Collector
```

Use Kubernetes/ECS/Nomad or another scheduler according to organizational standards.

---

# 39. Kubernetes considerations

A production Kubernetes deployment should normally include:

- separate API and worker Deployments;
- PodDisruptionBudgets;
- resource requests/limits;
- horizontal autoscaling;
- readiness/liveness/startup probes;
- graceful termination;
- workload identity;
- NetworkPolicies;
- secret injection;
- topology spread;
- anti-affinity where appropriate;
- OTel collector;
- ingress/WAF;
- egress restrictions.

Workers should stop claiming new work during shutdown and finish or safely relinquish current leases.

---

# 40. SLO examples

Example engineering targets—not universal guarantees:

```text
API availability
workflow admission latency
task claim p95
checkpoint p95
tool success rate
UNKNOWN outcome rate
approval notification latency
reconciliation recovery time
outbox publication lag
```

Define SLOs from actual business requirements rather than copying arbitrary numbers.

---

# 41. Capacity planning

Primary scaling dimensions:

```text
runs / second
tasks / run
checkpoint writes / task
tool calls / second
outbox events / second
audit events / second
average state size
retention period
regional distribution
```

Example estimation:

```text
100 runs/sec
x 20 tasks/run
= 2,000 task executions/sec
```

If each task produces multiple durable writes, database write volume is significantly higher than run admission rate.

Load-test the full state transition pattern rather than only HTTP throughput.

---

# 42. Hotspot avoidance

Bad pattern:

```text
UPDATE global_stats SET counter=counter+1
```

for every task.

That creates a contention hotspot.

Prefer:

- partitioned counters;
- streaming aggregation;
- append-oriented events;
- asynchronous analytics;
- per-tenant/per-shard aggregation.

Operational workflow state and analytical aggregation have different workloads.

---

# 43. Audit architecture

Audit should answer:

```text
Who initiated the run?
Which agent/model proposed the action?
Which policy allowed it?
Who approved it?
What exact action was approved?
Which tool executed it?
What provider reference resulted?
When did every transition occur?
```

Avoid blindly storing secrets, access tokens, or unnecessary sensitive payloads in audit rows.

---

# 44. Threat model highlights

Relevant threats include:

- prompt injection;
- malicious retrieved documents;
- tool argument manipulation;
- confused deputy;
- privilege escalation;
- cross-tenant access;
- replay attacks;
- duplicate side effects;
- stolen service credentials;
- poisoned memory;
- untrusted MCP/tool servers;
- SSRF;
- data exfiltration;
- approval spoofing;
- audit tampering.

Agent systems require conventional application security **plus** model/tool-specific controls.

---

# 45. MCP / tool-gateway integration

The provided gateway is HTTP-oriented, but the same execution ledger can wrap MCP tools.

Conceptually:

```text
Agent
 |
 v
Tool Gateway
 |
 +-- validate MCP tool schema
 +-- authorize tenant/principal
 +-- evaluate policy
 +-- approval if needed
 +-- reserve idempotency
 +-- invoke MCP server
 +-- persist result
 +-- audit
```

Do not give an arbitrary remote MCP server unrestricted enterprise credentials.

---

# 46. Agent-to-agent coordination

For multi-agent platforms:

```text
Supervisor
 |
 +-- Research Agent
 +-- Browser Agent
 +-- Data Agent
 +-- Coding Agent
 +-- Action Agent
```

Messages may be asynchronous, but task ownership and committed state should remain deterministic.

Useful durable records include:

```text
agent assignments
task dependencies
attempts
messages metadata
delegation
leases
results
artifacts
```

---

# 47. When distributed SQL is the wrong tool

Do not use it merely because the system contains AI.

A simple chatbot with:

```text
one request
one response
no durable action
no workflow
no multi-worker coordination
```

may only need ordinary PostgreSQL or even no persistent workflow database.

Distributed SQL becomes attractive when requirements include combinations of:

- high availability;
- horizontal scale;
- multi-region execution;
- strong transactions;
- durable workflows;
- tenant locality;
- regional resilience.

Architecture should follow requirements.

---

# 48. Development commands summary

```bash
# configure
cp .env.example .env

# start infrastructure/API
docker compose up --build

# detached
docker compose up --build -d

# status
docker compose ps

# logs
docker compose logs -f

# API logs
docker compose logs -f api

# stop
docker compose down

# stop and remove volumes
docker compose down -v

# tests
pytest -q

# lint
ruff check .

# local API
uvicorn app.main:app --reload

# worker
python -m app.workers.task_worker

# outbox publisher
python -m app.workers.outbox_worker
```

---

# 49. Troubleshooting

## API cannot connect to CockroachDB

Check:

```bash
docker compose ps
docker compose logs cockroach
```

Inside Compose use hostname:

```text
cockroach
```

From the host use:

```text
localhost
```

## Kafka connection fails

Check:

```bash
docker compose logs redpanda
```

Compose applications should use:

```text
redpanda:9092
```

Host applications should use the advertised listener appropriate to your local configuration.

## Database missing

The local app creates tables for convenience, but the database itself may need initialization depending on environment/version. Connect with Cockroach SQL and create it if required:

```sql
CREATE DATABASE IF NOT EXISTS agent_runtime;
```

Production environments should perform this through deployment/migration automation.

## Python imports fail

Activate the virtual environment and install editable dependencies:

```bash
source .venv/bin/activate
pip install -e ".[dev]"
```

## Port conflict

Inspect:

```bash
ss -lntp
```

and modify Compose port mappings if 8000, 8080, 9000, 9001, 9092, 6379, or 26257 is already occupied.

---

# 50. What must be added before real production use?

This repository demonstrates production-grade **patterns**, but production readiness requires environment-specific engineering.

Required areas include:

- corporate OIDC/service authentication;
- complete RBAC/ABAC;
- enforced tenant isolation;
- secrets manager/KMS;
- TLS/mTLS;
- real migration pipeline;
- Cockroach retry wrappers;
- lease renewal and fencing;
- provider-specific reconciliation;
- real model provider integration;
- durable LangGraph checkpoint adapter;
- Redis integration where beneficial;
- S3 artifact service;
- OpenTelemetry exporters;
- dashboards and alerting;
- circuit breakers;
- quotas/rate limits;
- retention/deletion;
- backup/restore;
- chaos testing;
- regional failover;
- penetration testing;
- dependency/image scanning;
- SBOM/signing;
- incident response;
- privacy/compliance review.

Calling software "production ready" without these deployment-specific controls would be misleading.

---

# 51. Suggested next implementation phases

### Phase 1 — execution correctness

```text
state machine
leases
versions
idempotency
approvals
outbox
audit
```

### Phase 2 — production integrations

```text
real LLM
LangGraph durable saver
Kafka consumers
S3 artifact manager
Redis cache
MCP gateway
OIDC
```

### Phase 3 — reliability

```text
reconciliation
fencing
retry framework
circuit breakers
rate limiting
dead-letter handling
chaos tests
```

### Phase 4 — global scale

```text
multi-region SQL
tenant locality
regional workers
failover
residency
capacity tests
```

### Phase 5 — governance

```text
policy engine
tool risk classes
approval matrices
retention
cost governance
model evaluation
security controls
```

---

# 52. Principal-engineering discussion points


1. **Probabilistic reasoning vs deterministic control.**
2. **Graph orchestration vs durable execution state.**
3. **Semantic memory vs transactional memory.**
4. **Message delivery vs committed workflow truth.**
5. **Retryable failure vs ambiguous side effect.**
6. **Lease ownership vs fencing.**
7. **Approval UX vs cryptographically/stably bound action authorization.**
8. **Local scalability vs multi-region consistency cost.**
9. **Event publication vs transactional outbox.**
10. **Agent autonomy vs policy-enforced authority.**

The central design principle is:

> Allow the model to decide what it proposes to do; allow deterministic infrastructure to decide what it is permitted to do and to record what actually happened.

---

# 53. Example enterprise scenario

Objective:

```text
Analyze Q3 sales,
generate leadership presentation,
email leadership,
schedule review.
```

Execution:

```text
API
 |
 v
create RUN
 |
 v
PLAN task READY
 |
 v
worker claims PLAN
 |
 v
LLM generates structured plan
 |
 v
checkpoint
 |
 v
DATA task
 |
 v
query enterprise data
 |
 v
ANALYZE
 |
 v
artifact stored in S3
 |
 v
SEND_EMAIL
 |
 v
policy says approval required
 |
 v
WAITING_APPROVAL
 |
 v
leader approves exact action hash
 |
 v
tool call STARTED
 |
 v
email provider
 |
 +---- success ------> SUCCEEDED
 |
 +---- timeout ------> UNKNOWN -> reconciliation
 |
 v
SCHEDULE_MEETING
 |
 v
RUN SUCCEEDED
```

At every step, SQL contains the durable state needed to explain, recover, audit, or continue execution.

---

# 54. Final architecture principle

A large-scale Agentic AI platform should not be designed as:

```text
LLM + tools + prompt history
```

A stronger model is:

```text
                Reasoning Plane
                     LLMs
                       |
                       v
                Orchestration Plane
                   LangGraph
                       |
                       v
                 Execution Plane
          durable tasks / workers / leases
                       |
                       v
                  Control Plane
       auth / policy / approvals / governance
                       |
                       v
                   Data Plane
 SQL / Redis / Vector / Graph / Kafka / S3
                       |
                       v
                 Enterprise Tools
```

Distributed SQL occupies a particularly important position: it is the **authoritative transactional memory and execution ledger** of the platform.

That makes durable agents recoverable, auditable, scalable, and substantially safer than systems whose execution truth exists only inside an LLM context window.

