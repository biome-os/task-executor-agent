"""
executor.py — SQLite-backed workflow store for the task-executor-agent.

Persists workflow plans and per-step state so execution can survive
agent restarts. All datetime values are stored as ISO 8601 UTC strings.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from models import StepStatus, WorkflowStatus

logger = logging.getLogger(__name__)

DB_PATH = Path("workflows.db")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class WorkflowStore:
    """
    Thread-safe SQLite store for workflow plans and step state.

    Open with ``open()`` before use and close with ``close()`` on shutdown.
    Designed to be called from a single asyncio task via asyncio.to_thread.
    """

    def __init__(self, db_path: Path = DB_PATH) -> None:
        self._db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None

    # ── Lifecycle ──────────────────────────────────────────────────────────

    def open(self) -> None:
        self._conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS workflows (
                task_id       TEXT PRIMARY KEY,
                title         TEXT NOT NULL,
                description   TEXT NOT NULL,
                goal          TEXT NOT NULL,
                requester_id  TEXT NOT NULL,
                status        TEXT NOT NULL DEFAULT 'pending',
                error         TEXT,
                created_at    TEXT NOT NULL,
                started_at    TEXT,
                completed_at  TEXT
            );

            CREATE TABLE IF NOT EXISTS workflow_steps (
                step_id         TEXT PRIMARY KEY,
                task_id         TEXT NOT NULL REFERENCES workflows(task_id),
                step_order      INTEGER NOT NULL,
                name            TEXT NOT NULL,
                description     TEXT NOT NULL,
                capability      TEXT NOT NULL,
                input_data      TEXT NOT NULL,
                depends_on      TEXT NOT NULL DEFAULT '[]',
                target_agent_id TEXT,
                status          TEXT NOT NULL DEFAULT 'pending',
                output_data     TEXT,
                error           TEXT,
                started_at      TEXT,
                completed_at    TEXT,
                duration_ms     REAL
            );

            CREATE INDEX IF NOT EXISTS idx_steps_task
                ON workflow_steps (task_id, step_order);
        """)
        self._conn.commit()
        logger.info("WorkflowStore opened: %s", self._db_path)

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    # ── Workflow CRUD ──────────────────────────────────────────────────────

    def create_workflow(self, plan: dict) -> str:
        """Insert a new workflow from a plan dict. Returns task_id."""
        task_id = plan["task_id"]
        now = _now_iso()
        self._conn.execute(
            """
            INSERT OR IGNORE INTO workflows
                (task_id, title, description, goal, requester_id, status, created_at)
            VALUES (?, ?, ?, ?, ?, 'pending', ?)
            """,
            (
                task_id,
                plan.get("title", ""),
                plan.get("description", ""),
                plan.get("goal", ""),
                plan.get("requester_id", ""),
                now,
            ),
        )
        for step in plan.get("steps", []):
            self._conn.execute(
                """
                INSERT OR IGNORE INTO workflow_steps
                    (step_id, task_id, step_order, name, description, capability,
                     input_data, depends_on, target_agent_id, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
                """,
                (
                    step["step_id"],
                    task_id,
                    step["order"],
                    step["name"],
                    step.get("description", ""),
                    step["capability"],
                    json.dumps(step.get("input_data", {})),
                    json.dumps(step.get("depends_on", [])),
                    step.get("target_agent_id"),
                ),
            )
        self._conn.commit()
        logger.info(
            "Workflow created: task_id=%s  steps=%d", task_id, len(plan.get("steps", []))
        )
        return task_id

    def update_workflow_status(
        self,
        task_id: str,
        status: WorkflowStatus,
        error: Optional[str] = None,
    ) -> None:
        now = _now_iso()
        if status == WorkflowStatus.executing:
            self._conn.execute(
                "UPDATE workflows SET status=?, started_at=COALESCE(started_at,?) WHERE task_id=?",
                (status.value, now, task_id),
            )
        elif status in (WorkflowStatus.completed, WorkflowStatus.failed):
            self._conn.execute(
                "UPDATE workflows SET status=?, completed_at=?, error=? WHERE task_id=?",
                (status.value, now, error, task_id),
            )
        else:
            self._conn.execute(
                "UPDATE workflows SET status=? WHERE task_id=?",
                (status.value, task_id),
            )
        self._conn.commit()

    def get_workflow(self, task_id: str) -> Optional[dict]:
        row = self._conn.execute(
            "SELECT * FROM workflows WHERE task_id=?", (task_id,)
        ).fetchone()
        if not row:
            return None
        return {**dict(row), "steps": self.get_steps(task_id)}

    def list_workflows(self, limit: int = 50) -> list[dict]:
        rows = self._conn.execute(
            "SELECT * FROM workflows ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    # ── Step CRUD ──────────────────────────────────────────────────────────

    def get_steps(self, task_id: str) -> list[dict]:
        rows = self._conn.execute(
            "SELECT * FROM workflow_steps WHERE task_id=? ORDER BY step_order",
            (task_id,),
        ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            for field in ("input_data", "output_data", "depends_on"):
                if d.get(field):
                    try:
                        d[field] = json.loads(d[field])
                    except (json.JSONDecodeError, TypeError):
                        pass
            result.append(d)
        return result

    def update_step_status(
        self,
        step_id: str,
        status: StepStatus,
        output_data: Optional[dict] = None,
        error: Optional[str] = None,
        duration_ms: Optional[float] = None,
        target_agent_id: Optional[str] = None,
    ) -> None:
        now = _now_iso()
        if status == StepStatus.running:
            self._conn.execute(
                """UPDATE workflow_steps SET status=?, started_at=COALESCE(started_at,?)
                   WHERE step_id=?""",
                (status.value, now, step_id),
            )
        elif status in (StepStatus.completed, StepStatus.failed, StepStatus.skipped):
            self._conn.execute(
                """UPDATE workflow_steps
                   SET status=?, completed_at=?, output_data=?, error=?,
                       duration_ms=?, target_agent_id=COALESCE(?,target_agent_id)
                   WHERE step_id=?""",
                (
                    status.value,
                    now,
                    json.dumps(output_data) if output_data is not None else None,
                    error,
                    duration_ms,
                    target_agent_id,
                    step_id,
                ),
            )
        else:
            self._conn.execute(
                "UPDATE workflow_steps SET status=? WHERE step_id=?",
                (status.value, step_id),
            )
        self._conn.commit()

    def get_step(self, step_id: str) -> Optional[dict]:
        row = self._conn.execute(
            "SELECT * FROM workflow_steps WHERE step_id=?", (step_id,)
        ).fetchone()
        if not row:
            return None
        d = dict(row)
        for field in ("input_data", "output_data", "depends_on"):
            if d.get(field):
                try:
                    d[field] = json.loads(d[field])
                except (json.JSONDecodeError, TypeError):
                    pass
        return d
