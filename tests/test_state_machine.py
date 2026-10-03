import pytest
from app.domain.state import TaskStatus, assert_task_transition

def test_valid_transition():
    assert_task_transition(TaskStatus.READY, TaskStatus.RUNNING)

def test_invalid_terminal_transition():
    with pytest.raises(ValueError):
        assert_task_transition(TaskStatus.SUCCEEDED, TaskStatus.RUNNING)
