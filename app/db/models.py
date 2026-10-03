import uuid
from datetime import datetime
from sqlalchemy import String, Integer, DateTime, ForeignKey, Text, Boolean, UniqueConstraint, Index
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func
from app.db.base import Base

def uid(): return uuid.uuid4()

class AgentRun(Base):
    __tablename__="agent_runs"
    id: Mapped[uuid.UUID]=mapped_column(UUID(as_uuid=True), primary_key=True, default=uid)
    tenant_id: Mapped[str]=mapped_column(String(128), index=True)
    objective: Mapped[str]=mapped_column(Text)
    status: Mapped[str]=mapped_column(String(32), default="PENDING", index=True)
    graph_version: Mapped[str]=mapped_column(String(64), default="v1")
    version: Mapped[int]=mapped_column(Integer, default=0)
    created_at: Mapped[datetime]=mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime]=mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

class Task(Base):
    __tablename__="tasks"
    id: Mapped[uuid.UUID]=mapped_column(UUID(as_uuid=True), primary_key=True, default=uid)
    run_id: Mapped[uuid.UUID]=mapped_column(ForeignKey("agent_runs.id", ondelete="CASCADE"), index=True)
    name: Mapped[str]=mapped_column(String(128))
    status: Mapped[str]=mapped_column(String(32), default="PENDING", index=True)
    payload: Mapped[dict]=mapped_column(JSONB, default=dict)
    result: Mapped[dict|None]=mapped_column(JSONB, nullable=True)
    version: Mapped[int]=mapped_column(Integer, default=0)
    lease_owner: Mapped[str|None]=mapped_column(String(128), nullable=True)
    lease_expires_at: Mapped[datetime|None]=mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime]=mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime]=mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    __table_args__=(Index("ix_tasks_claim", "status", "lease_expires_at"),)

class TaskAttempt(Base):
    __tablename__="task_attempts"
    id: Mapped[uuid.UUID]=mapped_column(UUID(as_uuid=True), primary_key=True, default=uid)
    task_id: Mapped[uuid.UUID]=mapped_column(ForeignKey("tasks.id", ondelete="CASCADE"), index=True)
    worker_id: Mapped[str]=mapped_column(String(128))
    status: Mapped[str]=mapped_column(String(32))
    error: Mapped[str|None]=mapped_column(Text, nullable=True)
    started_at: Mapped[datetime]=mapped_column(DateTime(timezone=True), server_default=func.now())
    finished_at: Mapped[datetime|None]=mapped_column(DateTime(timezone=True), nullable=True)

class Checkpoint(Base):
    __tablename__="checkpoints"
    id: Mapped[uuid.UUID]=mapped_column(UUID(as_uuid=True), primary_key=True, default=uid)
    run_id: Mapped[uuid.UUID]=mapped_column(ForeignKey("agent_runs.id", ondelete="CASCADE"), index=True)
    thread_id: Mapped[str]=mapped_column(String(128), index=True)
    graph_version: Mapped[str]=mapped_column(String(64))
    checkpoint_version: Mapped[int]=mapped_column(Integer)
    node: Mapped[str]=mapped_column(String(128))
    state: Mapped[dict]=mapped_column(JSONB)
    created_at: Mapped[datetime]=mapped_column(DateTime(timezone=True), server_default=func.now())
    __table_args__=(UniqueConstraint("run_id","checkpoint_version", name="uq_checkpoint_version"),)

class Approval(Base):
    __tablename__="approvals"
    id: Mapped[uuid.UUID]=mapped_column(UUID(as_uuid=True), primary_key=True, default=uid)
    run_id: Mapped[uuid.UUID]=mapped_column(ForeignKey("agent_runs.id", ondelete="CASCADE"), index=True)
    task_id: Mapped[uuid.UUID]=mapped_column(ForeignKey("tasks.id", ondelete="CASCADE"), index=True)
    principal_id: Mapped[str]=mapped_column(String(128))
    action_hash: Mapped[str]=mapped_column(String(64), index=True)
    nonce: Mapped[str]=mapped_column(String(64), unique=True)
    status: Mapped[str]=mapped_column(String(32), default="PROPOSED")
    expires_at: Mapped[datetime]=mapped_column(DateTime(timezone=True))
    decided_by: Mapped[str|None]=mapped_column(String(128), nullable=True)
    decided_at: Mapped[datetime|None]=mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime]=mapped_column(DateTime(timezone=True), server_default=func.now())

class ToolCall(Base):
    __tablename__="tool_calls"
    id: Mapped[uuid.UUID]=mapped_column(UUID(as_uuid=True), primary_key=True, default=uid)
    run_id: Mapped[uuid.UUID]=mapped_column(ForeignKey("agent_runs.id", ondelete="CASCADE"), index=True)
    task_id: Mapped[uuid.UUID]=mapped_column(ForeignKey("tasks.id", ondelete="CASCADE"), index=True)
    tool_name: Mapped[str]=mapped_column(String(128))
    idempotency_key: Mapped[str]=mapped_column(String(256), unique=True)
    request_hash: Mapped[str]=mapped_column(String(64))
    status: Mapped[str]=mapped_column(String(32), default="STARTED", index=True)
    provider_ref: Mapped[str|None]=mapped_column(String(256), nullable=True)
    response: Mapped[dict|None]=mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime]=mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime]=mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

class AuditEvent(Base):
    __tablename__="audit_events"
    id: Mapped[uuid.UUID]=mapped_column(UUID(as_uuid=True), primary_key=True, default=uid)
    tenant_id: Mapped[str]=mapped_column(String(128), index=True)
    run_id: Mapped[uuid.UUID|None]=mapped_column(UUID(as_uuid=True), nullable=True, index=True)
    actor: Mapped[str]=mapped_column(String(128))
    event_type: Mapped[str]=mapped_column(String(128), index=True)
    data: Mapped[dict]=mapped_column(JSONB, default=dict)
    created_at: Mapped[datetime]=mapped_column(DateTime(timezone=True), server_default=func.now())

class OutboxEvent(Base):
    __tablename__="outbox_events"
    id: Mapped[uuid.UUID]=mapped_column(UUID(as_uuid=True), primary_key=True, default=uid)
    aggregate_type: Mapped[str]=mapped_column(String(64))
    aggregate_id: Mapped[str]=mapped_column(String(64), index=True)
    event_type: Mapped[str]=mapped_column(String(128))
    payload: Mapped[dict]=mapped_column(JSONB)
    published: Mapped[bool]=mapped_column(Boolean, default=False, index=True)
    created_at: Mapped[datetime]=mapped_column(DateTime(timezone=True), server_default=func.now())
    published_at: Mapped[datetime|None]=mapped_column(DateTime(timezone=True), nullable=True)
