import asyncio
from datetime import datetime, timezone
from aiokafka import AIOKafkaProducer
from sqlalchemy import select
from app.core.config import settings
from app.db.models import OutboxEvent
from app.db.session import SessionLocal

async def loop():
    producer=AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    await producer.start()
    try:
        while True:
            async with SessionLocal() as db:
                q=(select(OutboxEvent).where(OutboxEvent.published.is_(False))
                   .order_by(OutboxEvent.created_at).with_for_update(skip_locked=True).limit(100))
                rows=(await db.execute(q)).scalars().all()
                if not rows:
                    await asyncio.sleep(.5); continue
                for event in rows:
                    import json
                    await producer.send_and_wait(settings.kafka_topic,
                        json.dumps({"id":str(event.id),"type":event.event_type,
                                    "payload":event.payload}).encode())
                    event.published=True; event.published_at=datetime.now(timezone.utc)
                await db.commit()
    finally:
        await producer.stop()

if __name__=="__main__": asyncio.run(loop())
