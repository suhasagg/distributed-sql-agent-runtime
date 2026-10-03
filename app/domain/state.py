from enum import StrEnum

class RunStatus(StrEnum):
    PENDING="PENDING"; RUNNING="RUNNING"; WAITING_APPROVAL="WAITING_APPROVAL"
    SUCCEEDED="SUCCEEDED"; FAILED="FAILED"; CANCELLED="CANCELLED"; UNKNOWN="UNKNOWN"

class TaskStatus(StrEnum):
    PENDING="PENDING"; READY="READY"; RUNNING="RUNNING"; WAITING_APPROVAL="WAITING_APPROVAL"
    SUCCEEDED="SUCCEEDED"; FAILED="FAILED"; CANCELLED="CANCELLED"; UNKNOWN="UNKNOWN"

class ApprovalStatus(StrEnum):
    PROPOSED="PROPOSED"; APPROVED="APPROVED"; REJECTED="REJECTED"; EXPIRED="EXPIRED"

class ToolCallStatus(StrEnum):
    STARTED="STARTED"; SUCCEEDED="SUCCEEDED"; FAILED="FAILED"; UNKNOWN="UNKNOWN"

TASK_TRANSITIONS = {
    TaskStatus.PENDING: {TaskStatus.READY, TaskStatus.CANCELLED},
    TaskStatus.READY: {TaskStatus.RUNNING, TaskStatus.CANCELLED},
    TaskStatus.RUNNING: {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.UNKNOWN,
                         TaskStatus.WAITING_APPROVAL, TaskStatus.READY, TaskStatus.CANCELLED},
    TaskStatus.WAITING_APPROVAL: {TaskStatus.READY, TaskStatus.CANCELLED, TaskStatus.FAILED},
    TaskStatus.UNKNOWN: {TaskStatus.READY, TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.CANCELLED},
    TaskStatus.FAILED: {TaskStatus.READY},
    TaskStatus.SUCCEEDED: set(),
    TaskStatus.CANCELLED: set(),
}

def assert_task_transition(old: TaskStatus, new: TaskStatus) -> None:
    if new not in TASK_TRANSITIONS[old]:
        raise ValueError(f"illegal task transition: {old} -> {new}")
