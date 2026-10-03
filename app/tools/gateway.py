from dataclasses import dataclass
from uuid import UUID
import httpx
from app.services.runtime_service import RuntimeService

@dataclass
class ToolResult:
    status: str
    response: dict|None = None
    provider_ref: str|None = None

class ToolGateway:
    def __init__(self, runtime: RuntimeService): self.runtime=runtime

    async def execute_http_action(self, *, run_id: UUID, task_id: UUID, tool_name: str,
                                  idempotency_key: str, url: str, payload: dict) -> ToolResult:
        call, is_new = await self.runtime.reserve_tool_call(
            run_id, task_id, tool_name, idempotency_key, payload
        )
        if not is_new:
            return ToolResult(call.status, call.response, call.provider_ref)

        try:
            async with httpx.AsyncClient(timeout=15) as client:
                r=await client.post(url, json=payload, headers={"Idempotency-Key":idempotency_key})
                r.raise_for_status()
                body=r.json() if "application/json" in r.headers.get("content-type","") else {"text":r.text[:2000]}
            call=await self.runtime.finish_tool_call(call.id, "SUCCEEDED", body, r.headers.get("x-request-id"))
            return ToolResult(call.status, call.response, call.provider_ref)
        except httpx.TimeoutException:
            # Outcome may be ambiguous. Reconcile with provider before retrying.
            call=await self.runtime.finish_tool_call(call.id, "UNKNOWN", {"reason":"timeout"})
            return ToolResult(call.status, call.response)
        except Exception as exc:
            call=await self.runtime.finish_tool_call(call.id, "FAILED", {"error":type(exc).__name__})
            return ToolResult(call.status, call.response)
