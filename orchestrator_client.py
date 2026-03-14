"""
orchestrator_client.py — WebSocket + HTTP client for the task-executor-agent.

Registers with the orchestrator, accepts execute_workflow task_requests,
executes steps sequentially, and emits workflow_event messages so the
orchestrator dashboard can trace execution in real-time.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import signal
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import httpx
import websockets
import websockets.exceptions

from executor import WorkflowStore
from models import StepStatus, WorkflowStatus

logger = logging.getLogger(__name__)

# ── Stable agent identity ──────────────────────────────────────────────────

_AGENT_ID_FILE = Path(".agent_id")


def _stable_agent_id() -> str:
    if _AGENT_ID_FILE.exists():
        return _AGENT_ID_FILE.read_text().strip()
    new_id = str(uuid.uuid4())
    _AGENT_ID_FILE.write_text(new_id)
    logger.info("Generated new stable agent ID: %s → %s", new_id, _AGENT_ID_FILE)
    return new_id


# ── Agent identity ─────────────────────────────────────────────────────────

AGENT_NAME = "task-executor-agent"
AGENT_VERSION = "1.0.0"
AGENT_DESCRIPTION = (
    "Orchestrates step-by-step execution of workflow plans received from the "
    "task-planner-agent. Dispatches individual task_requests to the best available "
    "agent for each step, tracks progress in SQLite, and reports real-time status "
    "back to the orchestrator via workflow_event messages."
)

REGISTRATION_PAYLOAD: dict = {
    "name": AGENT_NAME,
    "description": AGENT_DESCRIPTION,
    "version": AGENT_VERSION,
    "capabilities": [
        {
            "name": "execute_workflow",
            "description": "Execute a structured workflow plan from the task-planner-agent.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "plan": {
                        "type": "object",
                        "description": "WorkflowPlan dict produced by task-planner-agent.",
                    }
                },
                "required": ["plan"],
            },
            "tags": ["executor", "workflow", "orchestration"],
            "cost": {"type": "free", "estimated_cost_usd": None},
        },
    ],
    "tags": ["executor", "workflow", "orchestration"],
    "required_settings": [],
}

# ── Constants ──────────────────────────────────────────────────────────────

HEARTBEAT_INTERVAL_S: int = 15
MAX_BACKOFF_S: int = 60
DRAIN_TIMEOUT_S: int = 120
STEP_DISPATCH_TIMEOUT_S: float = 300.0


# ── Helpers ────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _envelope(
    sender_id: str,
    msg_type: str,
    payload: dict,
    recipient_id: Optional[str] = None,
    correlation_id: Optional[str] = None,
    msg_id: Optional[str] = None,
) -> str:
    return json.dumps({
        "id":             msg_id or str(uuid.uuid4()),
        "type":           msg_type,
        "sender_id":      sender_id,
        "recipient_id":   recipient_id,
        "payload":        payload,
        "timestamp":      _now_iso(),
        "correlation_id": correlation_id,
    })


# ── Main client ────────────────────────────────────────────────────────────

class OrchestratorClient:
    """
    Registers the task-executor-agent, handles execute_workflow requests,
    and orchestrates step-by-step execution.
    """

    def __init__(self, orchestrator_url: str = "http://localhost:8000") -> None:
        self._base = orchestrator_url.rstrip("/")
        self._http = httpx.AsyncClient(timeout=30)

        self._agent_id: str = ""
        self._ws_url: str = ""

        self._status: str = "starting"
        self._active_tasks: int = 0
        self._tasks_completed: int = 0
        self._tasks_failed: int = 0
        self._total_duration_ms: float = 0.0
        self._start_time: float = time.monotonic()

        self._shutting_down: bool = False
        self._current_ws: Any = None
        self._pending_responses: dict[str, asyncio.Future] = {}
        self._running_workflows: dict[str, asyncio.Task] = {}

        self._store = WorkflowStore()

    # ── Lifecycle ──────────────────────────────────────────────────────────

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self._graceful_shutdown()))

        self._store.open()
        await self._register()
        await self._connect_loop()

    # ── Registration ───────────────────────────────────────────────────────

    async def _register(self) -> None:
        url = f"{self._base}/api/v1/agents/register"
        logger.info("Registering with orchestrator at %s …", url)
        payload = {**REGISTRATION_PAYLOAD, "id": _stable_agent_id()}
        resp = await self._http.post(url, json=payload)
        resp.raise_for_status()
        data = resp.json()
        self._agent_id = data["agent_id"]
        self._ws_url = data["ws_url"]
        logger.info("Registered — agent_id=%s  ws=%s", self._agent_id, self._ws_url)

    # ── WebSocket loop ─────────────────────────────────────────────────────

    async def _connect_loop(self) -> None:
        backoff = 1.0
        while not self._shutting_down:
            try:
                logger.info("Connecting to %s …", self._ws_url)
                async with websockets.connect(self._ws_url) as ws:
                    backoff = 1.0
                    await self._run_session(ws)

            except websockets.exceptions.ConnectionClosed as exc:
                code = exc.rcvd.code if exc.rcvd else None
                if code == 4004:
                    logger.warning("Unknown agent_id (4004) — re-registering …")
                    try:
                        await self._register()
                    except Exception as reg_exc:
                        logger.error("Re-registration failed: %s", reg_exc)
                elif code == 4003:
                    logger.info("Agent is disabled by orchestrator (4003) — will retry so dashboard enable can restore connection")
                    backoff = max(backoff, 10.0)
                elif self._shutting_down:
                    break
                else:
                    logger.warning("WS closed (code=%s) — retry in %.0fs", code, backoff)

            except (OSError, Exception) as exc:
                if self._shutting_down:
                    break
                logger.warning("WS error (%s) — retry in %.0fs", exc, backoff)

            if not self._shutting_down:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF_S)

    async def _run_session(self, ws) -> None:
        self._current_ws = ws
        self._status = "available"
        logger.info("WebSocket session active")
        try:
            await asyncio.gather(
                self._heartbeat_loop(ws),
                self._recv_loop(ws),
            )
        finally:
            self._current_ws = None
            self._status = "offline"
            for fut in list(self._pending_responses.values()):
                if not fut.done():
                    fut.set_exception(ConnectionError("WebSocket session ended"))

    # ── Heartbeat ─────────────────────────────────────────────────────────

    async def _heartbeat_loop(self, ws) -> None:
        while True:
            await self._ws_send(ws, self._msg(
                "heartbeat",
                {
                    "status":       self._status,
                    "current_load": min(self._active_tasks / 5.0, 1.0),
                    "active_tasks": self._active_tasks,
                    "metrics":      self._metrics(),
                },
            ))
            await asyncio.sleep(HEARTBEAT_INTERVAL_S)

    # ── Receive loop ───────────────────────────────────────────────────────

    async def _recv_loop(self, ws) -> None:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning("Non-JSON frame ignored")
                continue

            mtype = msg.get("type", "?")
            logger.info("← [%s] from=%s", mtype, msg.get("sender_id", "?"))
            await self._dispatch(ws, msg)

    async def _dispatch(self, ws, msg: dict) -> None:
        mtype   = msg.get("type", "")
        payload = msg.get("payload", {})

        if mtype == "task_request":
            asyncio.create_task(self._handle_incoming_task(ws, msg))

        elif mtype == "task_response":
            corr = msg.get("correlation_id")
            if corr and corr in self._pending_responses:
                fut = self._pending_responses.pop(corr)
                if not fut.done():
                    fut.set_result(payload)

        elif mtype == "error":
            logger.error(
                "Orchestrator error [%s]: %s",
                payload.get("code"),
                payload.get("detail"),
            )
            original_id = payload.get("original_message_id")
            if original_id and original_id in self._pending_responses:
                fut = self._pending_responses.pop(original_id)
                if not fut.done():
                    fut.set_exception(RuntimeError(
                        f"[{payload.get('code')}] {payload.get('detail')}"
                    ))

        elif mtype == "task_cancel":
            task_id = payload.get("task_id", "")
            task = self._running_workflows.get(task_id)
            if task and not task.done():
                task.cancel()
                logger.info("Cancel requested for workflow %s", task_id)
            else:
                logger.warning("Cancel request for workflow %s — not running", task_id)

        elif mtype == "agent_registered":
            logger.info("Peer joined: %s", payload.get("agent_id"))

        elif mtype == "agent_offline":
            logger.info("Peer left: %s", payload.get("agent_id"))

        else:
            logger.debug("Unhandled message type: %r", mtype)

    # ── Incoming task handling ─────────────────────────────────────────────

    async def _handle_incoming_task(self, ws, msg: dict) -> None:
        req_id     = msg.get("id")
        sender_id  = msg.get("sender_id")
        payload    = msg.get("payload", {})
        capability = payload.get("capability")
        input_data = payload.get("input_data", {})

        self._active_tasks += 1
        self._status = "busy"
        t0 = time.monotonic()

        try:
            if capability == "execute_workflow":
                output, error = await self._cap_execute_workflow(input_data, sender_id, ws)
            elif capability == "get_workflow_status":
                output, error = await self._cap_get_workflow_status(input_data)
            elif capability == "list_workflows":
                output, error = await self._cap_list_workflows(input_data)
            else:
                output, error = None, f"Unknown capability: {capability!r}"

            duration_ms = (time.monotonic() - t0) * 1000

            if error:
                self._tasks_failed += 1
                await self._ws_send(ws, self._msg(
                    "task_response",
                    {"success": False, "error": error, "duration_ms": round(duration_ms, 1)},
                    recipient_id=sender_id,
                    correlation_id=req_id,
                ))
            else:
                self._tasks_completed += 1
                self._total_duration_ms += duration_ms
                await self._ws_send(ws, self._msg(
                    "task_response",
                    {"success": True, "output_data": output, "duration_ms": round(duration_ms, 1)},
                    recipient_id=sender_id,
                    correlation_id=req_id,
                ))

        except Exception as exc:
            duration_ms = (time.monotonic() - t0) * 1000
            self._tasks_failed += 1
            logger.exception("Unhandled error in capability %r", capability)
            await self._ws_send(ws, self._msg(
                "task_response",
                {"success": False, "error": str(exc), "duration_ms": round(duration_ms, 1)},
                recipient_id=sender_id,
                correlation_id=req_id,
            ))

        finally:
            self._active_tasks = max(0, self._active_tasks - 1)
            self._status = "draining" if self._shutting_down else (
                "busy" if self._active_tasks else "available"
            )
            await self._send_status_update(ws)

    # ── Capability: execute_workflow ───────────────────────────────────────

    async def _cap_execute_workflow(
        self, input_data: dict, requester_id: str, ws
    ) -> tuple[dict | None, str | None]:
        plan = input_data.get("plan")
        if not plan or not isinstance(plan, dict):
            return None, "input_data.plan is required and must be a dict"

        task_id = plan.get("task_id")
        if not task_id:
            return None, "plan.task_id is missing"

        steps = plan.get("steps", [])
        if not steps:
            return None, "plan.steps is empty — nothing to execute"

        # Persist plan
        await asyncio.to_thread(self._store.create_workflow, plan)

        # Start execution in background so we can respond to the planner quickly
        exec_task = asyncio.create_task(
            self._execute_workflow(task_id, steps, ws, plan=plan),
            name=f"exec-{task_id[:8]}",
        )
        self._running_workflows[task_id] = exec_task

        logger.info(
            "Workflow %s accepted for execution (%d steps)", task_id, len(steps)
        )
        return {
            "task_id":      task_id,
            "status":       "executing",
            "steps_total":  len(steps),
        }, None

    @staticmethod
    def _resolve_step_refs(input_data: Any, step_outputs: list[Optional[dict]]) -> Any:
        """
        Recursively substitute ``{{steps[N].output.field}}`` template references
        in *input_data* with values from *step_outputs* (0-indexed list of dicts).
        Works for strings, dicts, and lists; leaves other types unchanged.
        """
        _REF_RE = re.compile(r"\{\{steps\[(\d+)\]\.output\.([^}]+)\}\}")

        def _resolve_str(s: str) -> Any:
            """If *s* is a single reference, return the resolved value directly
            (preserving non-string types); otherwise do string substitution."""
            full_match = _REF_RE.fullmatch(s)
            if full_match:
                idx, path = int(full_match.group(1)), full_match.group(2).split(".")
                out = (step_outputs[idx] or {}) if idx < len(step_outputs) else {}
                for key in path:
                    if isinstance(out, dict):
                        out = out.get(key)
                    else:
                        out = None
                        break
                return out
            # Partial substitution — always returns a string
            def _sub(m: re.Match) -> str:
                idx, path = int(m.group(1)), m.group(2).split(".")
                out: Any = (step_outputs[idx] or {}) if idx < len(step_outputs) else {}
                for key in path:
                    if isinstance(out, dict):
                        out = out.get(key)
                    else:
                        out = None
                        break
                return str(out) if out is not None else m.group(0)
            return _REF_RE.sub(_sub, s)

        def _resolve(value: Any) -> Any:
            if isinstance(value, str):
                return _resolve_str(value)
            if isinstance(value, dict):
                return {k: _resolve(v) for k, v in value.items()}
            if isinstance(value, list):
                return [_resolve(v) for v in value]
            return value

        return _resolve(input_data)

    async def _execute_workflow(self, task_id: str, steps: list[dict], ws, plan: dict | None = None) -> None:
        """Execute all steps in order, emitting workflow_event messages throughout."""
        total = len(steps)
        completed = 0
        failed = 0
        # Track step outputs (0-indexed) so later steps can reference them
        step_outputs: list[Optional[dict]] = []

        logger.info("Starting execution of workflow %s (%d steps)", task_id, total)

        try:
            await asyncio.to_thread(
                self._store.update_workflow_status, task_id, WorkflowStatus.executing
            )

            # Emit workflow_started — include title/description/goal for the dashboard tracker
            await self._emit_workflow_event(ws, {
                "event":          "workflow_started",
                "task_id":        task_id,
                "title":          (plan or {}).get("title", ""),
                "description":    (plan or {}).get("description", ""),
                "goal":           (plan or {}).get("goal", ""),
                "total_steps":    total,
                "workflow_status": WorkflowStatus.executing.value,
            })

            for step in steps:
                step_id    = step["step_id"]
                step_order = step["order"]
                step_name  = step["name"]
                step_desc  = step.get("description", "")
                capability = step["capability"]
                # Resolve any {{steps[N].output.field}} references from previous step outputs
                input_data = self._resolve_step_refs(step.get("input_data", {}), step_outputs)

                logger.info(
                    "Workflow %s — step %d/%d: %r (capability=%s)",
                    task_id, step_order, total, step_name, capability,
                )

                # Mark step as running
                await asyncio.to_thread(
                    self._store.update_step_status, step_id, StepStatus.running
                )

                # Emit step_started — include input_data so dashboard can show the payload
                await self._emit_workflow_event(ws, {
                    "event":          "step_started",
                    "task_id":        task_id,
                    "step_id":        step_id,
                    "step_order":     step_order,
                    "step_name":      step_name,
                    "description":    step_desc,
                    "capability":     capability,
                    "input_data":     input_data,
                    "total_steps":    total,
                    "workflow_status": WorkflowStatus.executing.value,
                })

                t0 = time.monotonic()
                agent_id = None
                agent_name = None
                try:
                    target_agent_id = step.get("target_agent_id")
                    success, output, error, agent_id, agent_name = await self._dispatch_step(
                        capability=capability,
                        input_data=input_data,
                        target_agent_id=target_agent_id,
                        ws=ws,
                    )
                    duration_ms = (time.monotonic() - t0) * 1000

                    if success:
                        completed += 1
                        step_outputs.append(output)  # Record for subsequent step references
                        await asyncio.to_thread(
                            self._store.update_step_status,
                            step_id,
                            StepStatus.completed,
                            output_data=output,
                            duration_ms=round(duration_ms, 1),
                        )
                        logger.info(
                            "Workflow %s — step %d completed (%.0f ms)",
                            task_id, step_order, duration_ms,
                        )
                        await self._emit_workflow_event(ws, {
                            "event":          "step_completed",
                            "task_id":        task_id,
                            "step_id":        step_id,
                            "step_order":     step_order,
                            "step_name":      step_name,
                            "capability":     capability,
                            "output_data":    output,
                            "agent_id":       agent_id,
                            "agent_name":     agent_name,
                            "duration_ms":    round(duration_ms, 1),
                            "steps_completed": completed,
                            "total_steps":    total,
                            "workflow_status": WorkflowStatus.executing.value,
                        })

                    else:
                        failed += 1
                        step_outputs.append(None)  # Preserve index alignment
                        await asyncio.to_thread(
                            self._store.update_step_status,
                            step_id,
                            StepStatus.failed,
                            error=error,
                            duration_ms=round(duration_ms, 1),
                        )
                        logger.warning(
                            "Workflow %s — step %d failed: %s", task_id, step_order, error
                        )
                        await self._emit_workflow_event(ws, {
                            "event":          "step_failed",
                            "task_id":        task_id,
                            "step_id":        step_id,
                            "step_order":     step_order,
                            "step_name":      step_name,
                            "capability":     capability,
                            "error":          error,
                            "agent_id":       agent_id,
                            "agent_name":     agent_name,
                            "duration_ms":    round(duration_ms, 1),
                            "steps_failed":   failed,
                            "total_steps":    total,
                            "workflow_status": WorkflowStatus.executing.value,
                        })
                        # Mark remaining steps as skipped and abort workflow
                        remaining = steps[step_order:]  # steps after the failed one
                        for rem in remaining:
                            await asyncio.to_thread(
                                self._store.update_step_status, rem["step_id"], StepStatus.skipped
                            )
                        await asyncio.to_thread(
                            self._store.update_workflow_status,
                            task_id,
                            WorkflowStatus.failed,
                            error=f"Step {step_order} '{step_name}' failed: {error}",
                        )
                        await self._emit_workflow_event(ws, {
                            "event":             "workflow_failed",
                            "task_id":           task_id,
                            "steps_completed":   completed,
                            "steps_failed":      failed,
                            "total_steps":       total,
                            "error":             f"Step {step_order} failed: {error}",
                            "workflow_status":   WorkflowStatus.failed.value,
                        })
                        return

                except Exception as exc:
                    duration_ms = (time.monotonic() - t0) * 1000
                    failed += 1
                    step_outputs.append(None)  # Preserve index alignment
                    err_msg = str(exc)
                    await asyncio.to_thread(
                        self._store.update_step_status,
                        step_id,
                        StepStatus.failed,
                        error=err_msg,
                        duration_ms=round(duration_ms, 1),
                    )
                    logger.exception("Workflow %s — step %d raised exception", task_id, step_order)
                    await self._emit_workflow_event(ws, {
                        "event":          "step_failed",
                        "task_id":        task_id,
                        "step_id":        step_id,
                        "step_order":     step_order,
                        "step_name":      step_name,
                        "capability":     capability,
                        "error":          err_msg,
                        "agent_id":       agent_id,
                        "agent_name":     agent_name,
                        "duration_ms":    round(duration_ms, 1),
                        "workflow_status": WorkflowStatus.executing.value,
                    })
                    await asyncio.to_thread(
                        self._store.update_workflow_status,
                        task_id,
                        WorkflowStatus.failed,
                        error=f"Step {step_order} raised: {err_msg}",
                    )
                    await self._emit_workflow_event(ws, {
                        "event":           "workflow_failed",
                        "task_id":         task_id,
                        "steps_completed": completed,
                        "steps_failed":    failed,
                        "total_steps":     total,
                        "error":           err_msg,
                        "workflow_status": WorkflowStatus.failed.value,
                    })
                    return

            # All steps completed successfully
            await asyncio.to_thread(
                self._store.update_workflow_status, task_id, WorkflowStatus.completed
            )
            logger.info(
                "Workflow %s completed: %d/%d steps succeeded", task_id, completed, total
            )
            await self._emit_workflow_event(ws, {
                "event":           "workflow_completed",
                "task_id":         task_id,
                "steps_completed": completed,
                "steps_failed":    failed,
                "total_steps":     total,
                "workflow_status": WorkflowStatus.completed.value,
            })

        except asyncio.CancelledError:
            logger.info("Workflow %s cancelled", task_id)
            await asyncio.to_thread(
                self._store.update_workflow_status,
                task_id,
                WorkflowStatus.cancelled,
                error="Cancelled by user",
            )
            await self._emit_workflow_event(ws, {
                "event":           "workflow_cancelled",
                "task_id":         task_id,
                "steps_completed": completed,
                "steps_failed":    failed,
                "total_steps":     total,
                "error":           "Cancelled by user",
                "workflow_status": WorkflowStatus.cancelled.value,
            })
            # Don't re-raise — the task finishes cleanly after emitting the event

        finally:
            self._running_workflows.pop(task_id, None)

    # ── Step dispatch ──────────────────────────────────────────────────────

    async def _dispatch_step(
        self,
        capability: str,
        input_data: dict,
        target_agent_id: Optional[str],
        ws,
    ) -> tuple[bool, Optional[dict], Optional[str], Optional[str], Optional[str]]:
        """Discover best agent for *capability* and send a task_request.

        Returns (success, output, error, resolved_agent_id, resolved_agent_name).
        """
        ws_ref = self._current_ws
        if ws_ref is None:
            return False, None, "No active WebSocket connection", None, None

        # Resolve agent
        agent_name: Optional[str] = None
        if not target_agent_id:
            target_agent_id, agent_name = await self._discover_best(capability)
            if not target_agent_id:
                return False, None, f"No available agent for capability '{capability}'", None, None

        req_id = str(uuid.uuid4())
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        self._pending_responses[req_id] = fut

        try:
            await self._ws_send(ws_ref, _envelope(
                sender_id=self._agent_id,
                msg_type="task_request",
                payload={
                    "capability": capability,
                    "input_data": input_data,
                    "timeout_ms": STEP_DISPATCH_TIMEOUT_S * 1000,
                },
                recipient_id=target_agent_id,
                msg_id=req_id,
            ))

            resp_payload = await asyncio.wait_for(
                asyncio.shield(fut), timeout=STEP_DISPATCH_TIMEOUT_S
            )
            if resp_payload.get("success"):
                return True, resp_payload.get("output_data"), None, target_agent_id, agent_name
            return False, None, resp_payload.get("error", "Unknown error from agent"), target_agent_id, agent_name

        except asyncio.TimeoutError:
            return False, None, f"Step timed out after {STEP_DISPATCH_TIMEOUT_S:.0f}s", target_agent_id, agent_name
        except Exception as exc:
            return False, None, str(exc), target_agent_id, agent_name
        finally:
            self._pending_responses.pop(req_id, None)

    async def _discover_best(self, capability: str) -> tuple[Optional[str], Optional[str]]:
        """Return (agent_id, agent_name) for the best available agent."""
        try:
            resp = await self._http.get(
                f"{self._base}/api/v1/discover/best",
                params={"capability": capability},
            )
            if resp.status_code == 200:
                data = resp.json()
                agent_id = data.get("agent_id")
                agent_name = data.get("name")
                logger.info(
                    "Discovered agent %s (%s) for capability '%s'", agent_id, agent_name, capability
                )
                return agent_id, agent_name
            logger.warning("No agent for capability '%s' (status=%d)", capability, resp.status_code)
        except Exception as exc:
            logger.error("Discovery failed: %s", exc)
        return None, None

    # ── Workflow event emission ────────────────────────────────────────────

    async def _emit_workflow_event(self, ws, payload: dict) -> None:
        """Send a workflow_event message to the orchestrator."""
        try:
            await self._ws_send(ws, self._msg("workflow_event", payload))
        except Exception as exc:
            logger.warning("Failed to emit workflow_event: %s", exc)

    # ── Capability: get_workflow_status ────────────────────────────────────

    async def _cap_get_workflow_status(
        self, input_data: dict
    ) -> tuple[dict | None, str | None]:
        task_id = input_data.get("task_id", "").strip()
        if not task_id:
            return None, "input_data.task_id is required"
        workflow = await asyncio.to_thread(self._store.get_workflow, task_id)
        if workflow is None:
            return None, f"Workflow {task_id} not found"
        return {"workflow": workflow}, None

    # ── Capability: list_workflows ─────────────────────────────────────────

    async def _cap_list_workflows(
        self, input_data: dict
    ) -> tuple[dict | None, str | None]:
        limit = int(input_data.get("limit", 20))
        workflows = await asyncio.to_thread(self._store.list_workflows, limit)
        return {"workflows": workflows, "count": len(workflows)}, None

    # ── Status update ──────────────────────────────────────────────────────

    async def _send_status_update(self, ws) -> None:
        await self._ws_send(ws, self._msg(
            "status_update",
            {
                "status":       self._status,
                "current_load": min(self._active_tasks / 5.0, 1.0),
                "active_tasks": self._active_tasks,
                "metrics":      self._metrics(),
            },
        ))

    # ── Graceful shutdown ──────────────────────────────────────────────────

    async def _graceful_shutdown(self) -> None:
        if self._shutting_down:
            return
        self._shutting_down = True
        logger.info("Shutdown signal received — draining …")
        self._status = "draining"

        deadline = time.monotonic() + DRAIN_TIMEOUT_S
        while self._active_tasks > 0 and time.monotonic() < deadline:
            await asyncio.sleep(0.5)

        if self._agent_id:
            try:
                await self._http.delete(f"{self._base}/api/v1/agents/{self._agent_id}")
                logger.info("Deregistered from orchestrator.")
            except Exception as exc:
                logger.warning("Deregister failed: %s", exc)

        self._store.close()
        await self._http.aclose()
        logger.info("Shutdown complete.")

    # ── Helpers ────────────────────────────────────────────────────────────

    async def _ws_send(self, ws, msg_str: str) -> None:
        msg   = json.loads(msg_str)
        mtype = msg.get("type", "?")
        noisy = mtype in ("heartbeat", "status_update")
        log   = logger.debug if noisy else logger.info
        log("→ [%s] to=%s", mtype, msg.get("recipient_id") or "orchestrator")
        try:
            await ws.send(msg_str)
        except websockets.exceptions.ConnectionClosed:
            raise  # propagate → heartbeat loop exits → asyncio.gather raises → reconnect
        except Exception as exc:
            logger.warning("WS send failed: %s", exc)

    def _msg(
        self,
        msg_type: str,
        payload: dict,
        recipient_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
    ) -> str:
        return _envelope(self._agent_id, msg_type, payload, recipient_id, correlation_id)

    def _metrics(self) -> dict:
        n = self._tasks_completed + self._tasks_failed
        return {
            "tasks_completed":      self._tasks_completed,
            "tasks_failed":         self._tasks_failed,
            "avg_response_time_ms": (
                round(self._total_duration_ms / n, 1) if n else 0.0
            ),
            "uptime_seconds": round(time.monotonic() - self._start_time, 1),
        }
