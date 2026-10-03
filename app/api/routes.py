from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import SessionLocal
from app.services.runtime_service import RuntimeService

router=APIRouter(prefix="/v1")

async def get_db():
    async with SessionLocal() as s: yield s

class CreateRun(BaseModel):
    tenant_id: str=Field(min_length=1,max_length=128)
    objective: str=Field(min_length=1,max_length=20000)

class ApprovalDecision(BaseModel):
    actor: str
    approve: bool

@router.post("/runs")
async def create_run(body: CreateRun, db: AsyncSession=Depends(get_db)):
    run=await RuntimeService(db).create_run(body.tenant_id, body.objective)
    return {"run_id":str(run.id),"status":run.status}

@router.post("/approvals/{approval_id}/decision")
async def decide(approval_id: UUID, body: ApprovalDecision, db: AsyncSession=Depends(get_db)):
    try:
        a=await RuntimeService(db).decide_approval(approval_id, body.actor, body.approve)
        return {"approval_id":str(a.id),"status":a.status}
    except ValueError as e:
        raise HTTPException(409, str(e))
