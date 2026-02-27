"""
models.py — Shared data models for the task-executor-agent.
"""
from __future__ import annotations

from enum import Enum


class StepStatus(str, Enum):
    pending   = "pending"
    running   = "running"
    completed = "completed"
    failed    = "failed"
    skipped   = "skipped"


class WorkflowStatus(str, Enum):
    pending   = "pending"
    executing = "executing"
    completed = "completed"
    failed    = "failed"
    cancelled = "cancelled"


TERMINAL_STEP_STATUSES    = {StepStatus.completed, StepStatus.failed, StepStatus.skipped}
TERMINAL_WORKFLOW_STATUSES = {WorkflowStatus.completed, WorkflowStatus.failed, WorkflowStatus.cancelled}
