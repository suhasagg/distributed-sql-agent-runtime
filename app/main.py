from fastapi import FastAPI
from app.api.routes import router
from app.db.base import Base
from app.db.session import engine

app=FastAPI(title="Distributed SQL Agent Runtime", version="0.1.0")
app.include_router(router)

@app.on_event("startup")
async def startup():
    # Demo convenience. Use Alembic in real deployments.
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

@app.get("/health")
async def health(): return {"status":"ok"}
