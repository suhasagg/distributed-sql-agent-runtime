from typing import TypedDict
from langgraph.graph import StateGraph, END

class AgentState(TypedDict, total=False):
    objective: str
    plan: list[str]
    current: int
    output: dict

async def planner(state: AgentState):
    # Replace with your model client. Keep planning deterministic/structured at the boundary.
    return {"plan":["fetch_data","analyze","prepare_artifact"], "current":0}

async def supervisor(state: AgentState):
    return state

async def finalize(state: AgentState):
    return {"output":{"status":"complete","steps":state.get("plan",[])}}

def route(state: AgentState):
    return "finalize" if state.get("current",0) >= len(state.get("plan",[])) else "supervisor"

builder=StateGraph(AgentState)
builder.add_node("planner", planner)
builder.add_node("supervisor", supervisor)
builder.add_node("finalize", finalize)
builder.set_entry_point("planner")
builder.add_conditional_edges("planner", route, {"supervisor":"supervisor","finalize":"finalize"})
builder.add_edge("supervisor","finalize")
builder.add_edge("finalize", END)
graph=builder.compile()
