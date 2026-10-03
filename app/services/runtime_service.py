import hashlib, json, secrets
from datetime import datetime, timedelta, timezone
from uuid import UUID
from sqlalchemy import select, update, or_
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.models import AgentRun, Task, Approval, ToolCall, AuditEvent, OutboxEvent, Checkpoint
from app.domain.state import TaskStatus, assert_task_transition

def canonical_hash(payload: dict) -> str:
    raw=json.dumps(payload, sort_keys=True, separators=(",",":")).encode()
    return hashlib.sha256(raw).hexdigest()

class RuntimeService:
    def __init__(self, db: AsyncSession): self.db=db

    async def create_run(self, tenant_id: str, objective: str) -> AgentRun:
        run=AgentRun(tenant_id=tenant_id, objective=objective, status="RUNNING")
        self.db.add(run); await self.db.flush()
        for name, status in [("plan","READY"),("execute","PENDING"),("finalize","PENDING")]:
            self.db.add(Task(run_id=run.id, name=name, status=status, payload={}))
        self.db.add(OutboxEvent(aggregate_type="run", aggregate_id=str(run.id),
            event_type="RUN_CREATED", payload={"run_id":str(run.id),"tenant_id":tenant_id}))
        self.db.add(AuditEvent(tenant_id=tenant_id, run_id=run.id, actor="api",
            event_type="RUN_CREATED", data={"objective_hash":canonical_hash({"objective":objective})}))
        await self.db.commit()
        return run

    async def claim_task(self, worker_id: str, lease_seconds: int=60) -> Task|None:
        now=datetime.now(timezone.utc); until=now+timedelta(seconds=lease_seconds)
        q=(select(Task).where(Task.status=="READY")
           .where(or_(Task.lease_expires_at.is_(None), Task.lease_expires_at < now))
           .order_by(Task.created_at).with_for_update(skip_locked=True).limit(1))
        task=(await self.db.execute(q)).scalar_one_or_none()
        if not task: return None
        task.status="RUNNING"; task.lease_owner=worker_id; task.lease_expires_at=until; task.version+=1
        self.db.add(OutboxEvent(aggregate_type="task", aggregate_id=str(task.id),
            event_type="TASK_CLAIMED", payload={"task_id":str(task.id),"worker_id":worker_id}))
        await self.db.commit()
        return task

    async def transition_task(self, task_id: UUID, expected_version: int, new_status: TaskStatus,
                              result: dict|None=None) -> Task:
        task=(await self.db.execute(select(Task).where(Task.id==task_id))).scalar_one()
        assert_task_transition(TaskStatus(task.status), new_status)
        stmt=(update(Task).where(Task.id==task_id, Task.version==expected_version)
              .values(status=new_status.value, result=result, version=Task.version+1,
                      lease_owner=None, lease_expires_at=None).returning(Task))
        changed=(await self.db.execute(stmt)).scalar_one_or_none()
        if changed is None: raise RuntimeError("optimistic concurrency conflict")
        self.db.add(OutboxEvent(aggregate_type="task", aggregate_id=str(task_id),
            event_type="TASK_TRANSITIONED", payload={"from":task.status,"to":new_status.value}))
        await self.db.commit()
        return changed

    async def checkpoint(self, run_id: UUID, thread_id: str, graph_version: str,
                         checkpoint_version: int, node: str, state: dict) -> Checkpoint:
        cp=Checkpoint(run_id=run_id, thread_id=thread_id, graph_version=graph_version,
                      checkpoint_version=checkpoint_version, node=node, state=state)
        self.db.add(cp); await self.db.commit(); return cp

    async def request_approval(self, run_id: UUID, task_id: UUID, principal_id: str,
                               action: dict, ttl_seconds: int=3600) -> Approval:
        approval=Approval(run_id=run_id, task_id=task_id, principal_id=principal_id,
            action_hash=canonical_hash(action), nonce=secrets.token_hex(16),
            expires_at=datetime.now(timezone.utc)+timedelta(seconds=ttl_seconds))
        self.db.add(approval)
        self.db.add(OutboxEvent(aggregate_type="approval", aggregate_id=str(approval.id),
            event_type="APPROVAL_REQUESTED", payload={"run_id":str(run_id),"task_id":str(task_id)}))
        await self.db.commit(); return approval

    async def decide_approval(self, approval_id: UUID, actor: str, approve: bool) -> Approval:
        approval=(await self.db.execute(select(Approval).where(Approval.id==approval_id).with_for_update())).scalar_one()
        if approval.status != "PROPOSED": raise ValueError("approval already resolved")
        if approval.expires_at < datetime.now(timezone.utc):
            approval.status="EXPIRED"; await self.db.commit(); return approval
        approval.status="APPROVED" if approve else "REJECTED"
        approval.decided_by=actor; approval.decided_at=datetime.now(timezone.utc)
        self.db.add(OutboxEvent(aggregate_type="approval", aggregate_id=str(approval.id),
            event_type=f"APPROVAL_{approval.status}", payload={"approval_id":str(approval.id)}))
        await self.db.commit(); return approval

    async def reserve_tool_call(self, run_id: UUID, task_id: UUID, tool_name: str,
                                idempotency_key: str, request: dict) -> tuple[ToolCall,bool]:
        existing=(await self.db.execute(select(ToolCall).where(ToolCall.idempotency_key==idempotency_key))).scalar_one_or_none()
        if existing: return existing, False
        call=ToolCall(run_id=run_id, task_id=task_id, tool_name=tool_name,
                      idempotency_key=idempotency_key, request_hash=canonical_hash(request), status="STARTED")
        self.db.add(call); await self.db.commit()
        return call, True

    async def finish_tool_call(self, call_id: UUID, status: str, response: dict|None=None,
                               provider_ref: str|None=None) -> ToolCall:
        call=(await self.db.execute(select(ToolCall).where(ToolCall.id==call_id).with_for_update())).scalar_one()
        if call.status != "STARTED": return call
        call.status=status; call.response=response; call.provider_ref=provider_ref
        await self.db.commit(); return call
