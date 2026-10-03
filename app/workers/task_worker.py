import asyncio, os
from app.db.session import SessionLocal
from app.services.runtime_service import RuntimeService
from app.domain.state import TaskStatus
from app.core.config import settings

WORKER_ID=os.getenv("WORKER_ID","worker-1")

async def execute(task):
    # Dispatch to model/tool/domain handlers by task.name in a real system.
    await asyncio.sleep(0.05)
    return {"task":task.name,"ok":True}

async def loop():
    while True:
        async with SessionLocal() as db:
            svc=RuntimeService(db)
            task=await svc.claim_task(WORKER_ID, settings.lease_seconds)
            if not task:
                await asyncio.sleep(0.5); continue
            try:
                result=await execute(task)
                await svc.transition_task(task.id, task.version, TaskStatus.SUCCEEDED, result)
            except Exception as exc:
                try:
                    await svc.transition_task(task.id, task.version, TaskStatus.FAILED,
                                              {"error":type(exc).__name__})
                except Exception:
                    pass

if __name__=="__main__": asyncio.run(loop())
