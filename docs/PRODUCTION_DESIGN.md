# Production Design Notes

## Failure model

Design for:
- process crashes between DB writes and external calls
- duplicate queue delivery
- network partitions/timeouts
- stale workers
- model retries and nondeterminism
- deployment while workflows are in flight
- region loss
- downstream rate limiting
- partial provider outages

## Transaction boundaries

Keep transactions short. Never hold a SQL transaction open while waiting on an LLM or external API. Instead:

1. transactionally claim work
2. commit
3. perform computation/external call
4. transactionally record result

External effects require the tool-call ledger and idempotency strategy.

## Claiming work

`SELECT ... FOR UPDATE SKIP LOCKED` is a practical queue-claim primitive for moderate workloads. At very high scale, Kafka/Pulsar may distribute work while SQL remains authoritative. Consumers still need deduplication and state validation.

## Fencing

A lease alone does not stop a paused old worker from waking up. For high-risk resources, add a monotonically increasing fencing token and require downstream resource managers to reject stale tokens where possible.

## Approval TOCTOU

Approval must bind to an immutable action hash. If parameters change after approval, the hash changes and a new approval is required.

## Tool security

Tools should have:
- strict JSON schemas
- allowlisted destinations/actions
- per-tenant authorization
- risk classes
- approval requirements
- egress restrictions
- secret isolation
- payload size limits
- audit events

## LLM boundary

Never deserialize arbitrary model output into privileged execution without schema validation. The model proposes; deterministic software authorizes and executes.

## Observability dimensions

Every log/span/metric should carry where applicable:
- tenant_id
- run_id
- task_id
- attempt_id
- tool_call_id
- graph_version
- worker_id
- region
- model/provider
- trace_id

Measure:
- run success/failure/unknown rates
- task latency by type
- queue/claim latency
- approval wait time
- tool error/unknown rate
- reconciliation backlog
- token/cost consumption
- checkpoint size and write latency
- DB contention/retry rate

## CockroachDB-specific concerns

CockroachDB uses serializable isolation and may return retryable transaction errors under contention. Production repository code should wrap retryable transaction blocks with bounded retry/backoff. Avoid contention on single counters or "global" rows. Prefer UUIDs and partitionable access patterns.

Multi-region topology should be benchmarked with realistic latency. Strong transactions spanning regions have unavoidable speed-of-light cost; locality-aware schema and workflow ownership matter.

## Data retention

Separate operational state from long-lived audit/analytics. Apply retention, archival, deletion, and legal-hold rules independently. Large prompt/response bodies can be encrypted in object storage while SQL stores hashes and metadata.

## Disaster recovery

Define RPO/RTO per state class. Test:
- backup restoration
- accidental deletion recovery
- region failover
- Kafka replay
- tool-call reconciliation after outage
- restoring workflows without duplicating external actions
