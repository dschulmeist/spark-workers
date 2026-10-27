import asyncio
import json
import logging
import shutil
import sqlite3
import time
import uuid
from collections import Counter
from copy import deepcopy
from pathlib import Path

import httpx
from pydantic import BaseModel, ConfigDict, Field, model_validator

from . import workspace
from .opencode import OpenCode
from .verification import Check, run_check

LOG = logging.getLogger(__name__)
ACTIVE = {"preparing", "running", "waiting_permission", "cancelling", "recovering"}
TERMINAL = {"completed", "failed", "cancelled"}


class Assignment(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_id: str = Field(
        min_length=1, max_length=100, description="Stable idempotency key; reuse when retrying the same start"
    )
    repository: str = Field(description="Absolute Git repository root on the OpenCode host")
    base_commit: str = Field(
        min_length=7, description="Explicit commit SHA, not a branch name; normally current HEAD"
    )
    include_working_tree: bool = True
    goal: str = Field(min_length=10, max_length=12000)
    relevant_files: list[str] = Field(min_length=1, max_length=50)
    allowed_paths: list[str] = Field(
        max_length=50,
        description="Exact files or directory prefixes the worker may change; empty means read-only",
    )
    acceptance_criteria: list[str] = Field(min_length=1, max_length=20)

    @model_validator(mode="after")
    def validate_assignment(self):
        if not Path(self.repository).is_absolute():
            raise ValueError("repository must be absolute")
        if any(c not in "0123456789abcdefABCDEF" for c in self.base_commit):
            raise ValueError("base_commit must be a commit SHA")
        workspace.validate_paths(self.relevant_files + self.allowed_paths)
        return self


class BackendHealth:
    def __init__(self, settings):
        self.settings = settings
        self.available = False
        self.failures = 0
        self.checked_at = None
        self.error = "Not checked"

    async def probe(self, client, threshold):
        self.checked_at = time.time()
        try:
            response = await client.get(self.settings["base_url"] + "/models", timeout=3)
            response.raise_for_status()
            models = response.json()["data"]
            if self.settings["model"] not in {m["id"] for m in models}:
                raise ValueError("Expected model is not loaded")
            self.available, self.failures, self.error = True, 0, None
            return True
        except (httpx.HTTPError, ValueError, KeyError, TypeError) as error:
            self.failures += 1
            self.error = str(error)[:400]
            if self.failures >= threshold:
                self.available = False
            return False


def capacity(backend, health, settings):
    if not health[backend].available:
        return 0
    dependency = settings[backend].get("expand_when_unavailable")
    if dependency and not health[dependency].available:
        return settings[backend]["offline_capacity"]
    return settings[backend]["capacity"]


def run_guidance(turn_limit):
    return (
        f"This run has a work budget of {turn_limit} assistant turns and one additional turn for the final JSON result. "
        "Give brief progress checkpoints between batches without raw logs or secrets. "
        "If the scope does not fit, report what you checked and the remaining gaps. "
    )


def assignment_prompt(job, turn_limit):
    task = {key: job[key] for key in ["goal", "relevant_files", "allowed_paths", "acceptance_criteria"]}
    return (
        "You are a delegated worker, not the coordinator. Work only on this bounded assignment. "
        "Read applicable AGENTS.md/CLAUDE.md instructions. Do not delegate or start more agents. "
        "Do not push, publish, message external services, or change repository configuration. "
        "Stay in the provided worktree. Do not access the original checkout. "
        "Shell commands require coordinator approval. Never bypass a denied permission. "
        "Only edit allowed_paths (directory prefixes are allowed). An empty list means no edits. "
        + run_guidance(turn_limit)
        + "Keep outputs short and inspect only relevant paths. "
        "Report uncertainty instead of expanding scope. Do not commit. "
        "Finish in at most 500 words with a JSON object with summary, changes, tests (commands and outcomes), "
        "and open_points. Do not include full logs.\n\n" + json.dumps(task, ensure_ascii=False, indent=2)
    )


def turn_limit_reached(assistants, limit):
    return any(message.get("finish") in {"tool-calls", "tool_calls"} for message in assistants[limit:])


def active_elapsed(job, now):
    started = job.get("submitted_at")
    if started is None:
        started = job.get("created_at", now)
    waiting = job.get("permission_wait_seconds", 0)
    if job.get("permission_wait_started_at") is not None:
        waiting += max(0, now - job["permission_wait_started_at"])
    return max(0, now - started - waiting)


def finish_permission_wait(job, now):
    started = job.pop("permission_wait_started_at", None)
    if started is not None:
        job["permission_wait_seconds"] = job.get("permission_wait_seconds", 0) + max(0, now - started)


class Manager:
    def __init__(self, root, config, client):
        self.root = Path(root)
        self.config = config
        self.client = client
        self.opencode = OpenCode(client)
        self.state = self.root / ".state"
        self.state.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.state / "workers.sqlite3")
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, request_id TEXT UNIQUE NOT NULL, data TEXT NOT NULL)"
        )
        self.db.commit()
        self.health = {key: BackendHealth(value) for key, value in config["backends"].items()}
        self.lock = asyncio.Lock()
        self.background = set()
        self.refresh_locks = {}

    def jobs(self):
        return [json.loads(row[0]) for row in self.db.execute("SELECT data FROM jobs ORDER BY rowid DESC")]

    def list_tasks(
        self, status=None, repository=None, created_after=None, created_before=None, limit=20, offset=0
    ):
        if created_after is not None and created_before is not None and created_after > created_before:
            raise ValueError("created_after must not exceed created_before")
        predicates, parameters = [], []
        for expression, value in [
            ("json_extract(data, '$.status') = ?", status),
            ("json_extract(data, '$.repository') = ?", repository),
            ("json_extract(data, '$.created_at') >= ?", created_after),
            ("json_extract(data, '$.created_at') <= ?", created_before),
        ]:
            if value is not None:
                predicates.append(expression)
                parameters.append(value)
        where = " AND ".join(predicates) or "1"
        total = self.db.execute(f"SELECT COUNT(*) FROM jobs WHERE {where}", parameters).fetchone()[0]
        rows = self.db.execute(
            f"SELECT data FROM jobs WHERE {where} ORDER BY rowid DESC LIMIT ? OFFSET ?",
            [*parameters, limit, offset],
        )
        tasks = []
        for row in rows:
            job = json.loads(row[0])
            task = {
                key: job[key]
                for key in [
                    "id",
                    "backend",
                    "status",
                    "repository",
                    "created_at",
                    "finished_at",
                    "run",
                    "cleaned_up",
                ]
                if key in job
            }
            task["goal"] = job["goal"][:180]
            if job.get("error"):
                task["error"] = job["error"][:240]
            tasks.append(task)
        next_offset = offset + len(tasks)
        return {"tasks": tasks, "total": total, "next_offset": next_offset if next_offset < total else None}

    def get(self, task_id):
        row = self.db.execute("SELECT data FROM jobs WHERE id=?", (task_id,)).fetchone()
        if not row:
            raise ValueError("Unknown task_id")
        return json.loads(row[0])

    def save(self, job):
        job["updated_at"] = time.time()
        self.db.execute(
            "INSERT INTO jobs VALUES (?, ?, ?) ON CONFLICT(id) DO UPDATE SET data=excluded.data",
            (job["id"], job["request_id"], json.dumps(job)),
        )
        self.db.commit()

    def status(self):
        counts = Counter(job["backend"] for job in self.jobs() if job["status"] in ACTIVE)
        return {
            key: {
                "available": value.available,
                "capacity": capacity(key, self.health, self.config["backends"]),
                "active": counts[key],
                "free": max(0, capacity(key, self.health, self.config["backends"]) - counts[key]),
                "checked_at": value.checked_at,
                "last_error": value.error,
            }
            for key, value in self.health.items()
        }

    async def probe(self):
        await asyncio.gather(
            *(h.probe(self.client, self.config["health_failure_threshold"]) for h in self.health.values())
        )

    async def start(self, backend, arguments):
        assignment = Assignment.model_validate(arguments)
        if backend not in self.health:
            raise ValueError("Unknown backend; select a reachable backend from workers_status")
        async with self.lock:
            previous = next((job for job in self.jobs() if job["request_id"] == assignment.request_id), None)
            if previous:
                if previous["backend"] != backend or any(
                    previous[key] != value for key, value in assignment.model_dump().items()
                ):
                    raise ValueError("request_id already belongs to a different assignment")
                return self.public(previous)
            if not await self.health[backend].probe(self.client, self.config["health_failure_threshold"]):
                raise ValueError("Backend is not reachable; no task was started")
            if self.status()[backend]["free"] < 1:
                raise ValueError("All worker slots on this backend are occupied")
            job = {
                **assignment.model_dump(),
                "id": uuid.uuid4().hex,
                "backend": backend,
                "status": "preparing",
                "run": 1,
                "created_at": time.time(),
                "session_id": None,
                "turns_used": 0,
                "turn_limit": self.config["max_assistant_turns"],
                "final_turn_grace": 1,
                "elapsed_seconds": 0,
                "time_limit_seconds": self.config["task_timeout_seconds"],
            }
            self.save(job)
            background = asyncio.create_task(self.prepare(job["id"]))
            self.background.add(background)
            background.add_done_callback(self.background.discard)
            return self.public(job)

    async def prepare(self, task_id):
        job = self.get(task_id)
        artifact = self.state / "jobs" / task_id
        artifact.mkdir(parents=True)
        worktree = artifact / "worktree"
        try:
            result = await asyncio.to_thread(
                workspace.prepare,
                job["repository"],
                job["base_commit"],
                job["include_working_tree"],
                worktree,
                artifact,
            )
            job = self.get(task_id)
            job["workspace"] = result
            self.save(job)
            if job["status"] == "cancelling":
                await self.finalize(job, "cancelled")
                return
            backend = self.config["backends"][job["backend"]]
            model = {"providerID": backend["provider"], "id": backend["model"]}
            job["session_id"] = "ses_" + task_id
            self.save(job)
            session = await self.opencode.create(
                worktree,
                model,
                workspace.permissions(worktree, job["allowed_paths"]),
                f"Spark worker {task_id[:8]}",
                job["session_id"],
            )
            job = self.get(task_id)
            job["session_id"] = session["id"]
            job["session_confirmed"] = True
            if job["status"] != "preparing":
                self.save(job)
                if job["status"] == "cancelling":
                    await self.cancel(task_id)
                return
            job["submitted_at"] = time.time()
            job["status"] = "running"
            self.save(job)
            await self.send_prompt(task_id, assignment_prompt(job, job["turn_limit"]))
        except Exception as error:
            LOG.exception("Worker preparation failed: %s", task_id)
            job = self.get(task_id)
            job["error"] = str(error)[:1200]
            if job["session_id"]:
                job["status"] = "recovering"
                job["stop_reason"] = "Prompt admission uncertain; stop before releasing capacity"
                self.save(job)
                try:
                    await self.cancel(task_id)
                except httpx.HTTPStatusError as stop_error:
                    if stop_error.response.status_code == 404 and not job.get("session_confirmed"):
                        await self.finalize(job, "failed")
                    else:
                        LOG.exception("Could not confirm worker stop: %s", task_id)
            else:
                if worktree.exists():
                    job["workspace"] = {
                        "repository": job["repository"],
                        "worktree": str(worktree),
                        "snapshot_commit": workspace.git(worktree, "rev-parse", "HEAD").decode().strip(),
                    }
                await self.finalize(job, "failed")

    async def send_prompt(self, task_id, text, *, dispatch_prepared=False):
        job = self.get(task_id)
        if not dispatch_prepared:
            job["dispatch"] = {"id": "msg_" + uuid.uuid4().hex, "status": "pending"}
            self.save(job)
        try:
            await self.opencode.prompt(job["session_id"], text, job["dispatch"]["id"])
        except BaseException:
            job = self.get(task_id)
            job["dispatch"]["status"] = "uncertain"
            job["status"] = "cancelling"
            job["stop_reason"] = "Prompt admission uncertain; waiting for positive admission evidence"
            self.save(job)
            raise
        job = self.get(task_id)
        job["dispatch"]["status"] = "accepted"
        self.save(job)
        if job["status"] == "cancelling":
            await self.refresh(task_id)

    async def refresh(self, task_id):
        async with self.refresh_locks.setdefault(task_id, asyncio.Lock()):
            return await self._refresh(task_id)

    async def result(self, task_id, run=None):
        job = self.get(task_id)
        if run is None or run == job.get("run", 1):
            return self.public(await self.refresh(task_id))
        path = self.state / "jobs" / task_id / "runs" / str(run) / "result.json"
        if not path.is_file():
            raise ValueError("No recorded result for this run")
        return json.loads(path.read_text())

    def archive_run(self, job):
        folder = self.state / "jobs" / job["id"] / "runs" / str(job.get("run", 1))
        result_path = folder / "result.json"
        if result_path.exists():
            return
        folder.mkdir(parents=True, exist_ok=True)
        snapshot = deepcopy(self.public(job))
        if job.get("artifacts"):
            shutil.copyfile(job["artifacts"]["patch"], folder / "changes.patch")
            snapshot["artifacts"]["patch"] = str(folder / "changes.patch")
        evidence = folder.parent.parent / "opencode.json"
        if evidence.is_file():
            shutil.copyfile(evidence, folder / "opencode.json")
            snapshot["evidence"] = str(folder / "opencode.json")
        snapshot.update(historical=True, recorded_at=time.time())
        temporary = folder / "result.json.tmp"
        temporary.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2))
        temporary.replace(result_path)

    async def wait(self, task_ids, mode, timeout_seconds):
        deadline = time.monotonic() + timeout_seconds
        jobs = [self.get(task_id) for task_id in task_ids]
        while True:
            finished = [job["status"] in TERMINAL for job in jobs]
            ready = any(finished) if mode == "any" else all(finished)
            remaining = deadline - time.monotonic()
            if ready or remaining <= 0 or any(job["status"] == "waiting_permission" for job in jobs):
                return {
                    "ready": ready,
                    "timed_out": not ready and remaining <= 0,
                    "tasks": [
                        {
                            key: job[key]
                            for key in [
                                "id",
                                "backend",
                                "status",
                                "progress",
                                "activity",
                                "turns_used",
                                "turn_limit",
                                "final_turn_grace",
                                "elapsed_seconds",
                                "time_limit_seconds",
                                "permission_requests",
                            ]
                            if key in job
                        }
                        for job in jobs
                    ],
                }
            await asyncio.sleep(min(1, remaining))
            jobs = [self.get(task_id) for task_id in task_ids]

    async def _refresh(self, task_id):
        job = self.get(task_id)
        session = job.get("session_id")
        if job["status"] not in ACTIVE or not session:
            return job
        if job.get("dispatch", {}).get("status") == "pending":
            return job
        try:
            info = await self.opencode.info(session)
            if job["status"] == "preparing":
                return job
            messages = await self.opencode.messages(session)
            dispatch = job.get("dispatch")
            inbox = await self.opencode.inbox(session) if dispatch else []
            if dispatch and dispatch["status"] == "uncertain":
                if not any(item.get("id") == dispatch["id"] for item in [*inbox, *messages]):
                    return self.get(task_id)
                dispatch["status"] = "accepted"
                job["status"] = self.get(task_id)["status"]
                self.save(job)
            artifact = self.state / "jobs" / task_id
            evidence = artifact / "opencode.json"
            temporary = artifact / "opencode.json.tmp"
            temporary.write_text(
                json.dumps({"session": info, "messages": messages}, ensure_ascii=False, indent=2)
            )
            temporary.replace(evidence)
            job["evidence"] = str(evidence)
            assistants = [m for m in messages if m.get("type") == "assistant"]
            run_assistants = [
                m
                for m in assistants
                if m.get("time", {}).get("created", 0) >= job.get("submitted_at", 0) * 1000
            ]
            progress = self.summary(run_assistants)
            if progress:
                job["progress"] = progress[:1200]
            for message in reversed(run_assistants):
                tools = [part for part in message.get("content", []) if part.get("type") == "tool"]
                if tools:
                    tool = tools[-1]
                    job["activity"] = {
                        "tool": tool.get("name", "unknown"),
                        "status": tool.get("state", {}).get("status", "unknown"),
                    }
                    break
            job["tokens"] = info.get("tokens")
            job["cost_reported_by_opencode"] = info.get("cost")
            job.setdefault("turn_limit", self.config["max_assistant_turns"])
            job.setdefault("final_turn_grace", 1)
            job.setdefault("time_limit_seconds", self.config["task_timeout_seconds"])
            job["turns_used"] = len(run_assistants)
            active = (await self.opencode.request("GET", "/api/session/active"))["data"]
            idle_at = info.get("time", {}).get("idle", 0) / 1000
            idle = idle_at >= job.get("submitted_at", float("inf"))
            queued = dispatch and any(item.get("id") == dispatch["id"] for item in inbox)
            failed_response = bool(
                run_assistants
                and run_assistants[-1].get("finish") == "error"
                and run_assistants[-1].get("error")
                and run_assistants[-1].get("time", {}).get("completed", 0) / 1000
                >= job.get("submitted_at", float("inf"))
            )
            finished_at = None
            if session not in active and not queued:
                if idle:
                    finished_at = idle_at
                elif run_assistants and run_assistants[-1].get("finish") == "error":
                    completed = run_assistants[-1].get("time", {}).get("completed", 0) / 1000
                    if completed >= job.get("submitted_at", float("inf")):
                        finished_at = completed
            now = time.time()
            terminal = session not in active and not queued and (idle or failed_response)
            requests = (
                [] if terminal or job["status"] == "cancelling" else await self.opencode.permissions(session)
            )
            if requests:
                job.setdefault("permission_wait_started_at", now)
            budget_time = min(now, finished_at) if finished_at is not None else now
            job["elapsed_seconds"] = round(active_elapsed(job, budget_time), 2)
            expected = self.config["backends"][job["backend"]]
            current = self.get(task_id)
            if current["status"] == "cancelling":
                job["status"] = "cancelling"
                if current.get("cancel_requested") or not current.get("stop_reason"):
                    job["cancel_requested"] = True
                if current.get("stop_reason"):
                    job["stop_reason"] = current["stop_reason"]
            else:
                if any(
                    m.get("model", {}).get("providerID") != expected["provider"]
                    or m.get("model", {}).get("id") != expected["model"]
                    for m in assistants
                ):
                    job["stop_reason"] = "Unexpected model selected; no cloud fallback allowed"
                if not requests and job["elapsed_seconds"] > job["time_limit_seconds"]:
                    job["stop_reason"] = "Worker time limit reached"
                if turn_limit_reached(run_assistants, job["turn_limit"]):
                    job["stop_reason"] = "Worker turn limit reached"
            if job.get("stop_reason") and job["status"] != "cancelling":
                job["status"] = "cancelling"
                self.save(job)
            if job["status"] == "cancelling":
                if dispatch:
                    await self.opencode.cancel_input(session, dispatch["id"])
                await self.opencode.interrupt(session)
                if dispatch:
                    inbox = await self.opencode.inbox(session)
                active = (await self.opencode.request("GET", "/api/session/active"))["data"]
            current = self.get(task_id)
            if current["status"] == "cancelling":
                job["status"] = "cancelling"
                if current.get("cancel_requested"):
                    job["cancel_requested"] = True
                if current.get("stop_reason"):
                    job["stop_reason"] = current["stop_reason"]
            queued = dispatch and any(item.get("id") == dispatch["id"] for item in inbox)
            if (
                session not in active
                and not queued
                and (idle or failed_response or job["status"] == "cancelling")
            ):
                job["summary"] = self.summary(run_assistants)
                response_error = run_assistants[-1].get("error") if run_assistants else None
                manual_cancel = job.get("cancel_requested") and not job.get("stop_reason")
                if response_error and not manual_cancel:
                    job["opencode_error"] = {
                        "type": response_error["type"][:100],
                        "message": response_error["message"][:1200],
                        **{
                            key: response_error[key]
                            for key in ["status", "retryAfterMs"]
                            if key in response_error
                        },
                    }
                    job["error"] = job["opencode_error"]["message"]
                outcome = "cancelled" if job["status"] == "cancelling" else "completed"
                if not manual_cancel and (info.get("outcome") == "failed" or job.get("stop_reason")):
                    outcome = "failed"
                if outcome == "completed" and (
                    not run_assistants
                    or run_assistants[-1].get("finish") != "stop"
                    or not self.summary([run_assistants[-1]])
                ):
                    outcome = "failed"
                    job.setdefault("error", "Session ended without a final assistant response")
                if job.get("stop_reason"):
                    job["error"] = job["stop_reason"]
                await self.finalize(job, outcome)
            else:
                tool_inputs = {
                    part["id"]: part.get("state", {}).get("input")
                    for message in assistants
                    for part in message.get("content", [])
                    if part.get("type") == "tool"
                }
                for request in requests:
                    request["tool_input"] = tool_inputs.get(request.get("source", {}).get("id"))
                job["permission_requests"] = requests
                current = self.get(task_id)
                if current["status"] == "cancelling":
                    job["status"] = "cancelling"
                    if current.get("cancel_requested"):
                        job["cancel_requested"] = True
                    if current.get("stop_reason"):
                        job["stop_reason"] = current["stop_reason"]
                else:
                    job["status"] = "waiting_permission" if requests else "running"
                now = time.time()
                if requests and job["status"] != "cancelling":
                    job.setdefault("permission_wait_started_at", now)
                else:
                    finish_permission_wait(job, now)
                job["elapsed_seconds"] = round(active_elapsed(job, now), 2)
                self.save(job)
        except (httpx.HTTPError, OSError, ValueError, KeyError) as error:
            current = self.get(task_id)
            current["connection_error"] = str(error)[:600]
            self.save(current)
        return self.get(task_id)

    @staticmethod
    def summary(assistants):
        for message in reversed(assistants):
            text = "\n".join(
                p.get("text", "") for p in message.get("content", []) if p.get("type") == "text"
            ).strip()
            if text:
                return text[:10000]
        return ""

    async def finalize(self, job, status):
        if job.get("workspace"):
            try:
                job["artifacts"] = await asyncio.to_thread(
                    workspace.export_changes, job, self.state / "jobs" / job["id"]
                )
                if job["artifacts"]["scope_violations"]:
                    status = "failed"
                    job["error"] = "Worker changed files outside allowed_paths; inspect retained patch"
            except Exception as error:
                status = "failed"
                job["error"] = f"Artifact export failed: {error}"
        job["status"] = status
        job.pop("permission_requests", None)
        job["finished_at"] = time.time()
        job["duration_seconds"] = round(job["finished_at"] - job["created_at"], 2)
        self.save(job)

    async def cancel(self, task_id):
        job = self.get(task_id)
        if job["status"] not in ACTIVE:
            return self.public(job)
        if not job.get("stop_reason"):
            job["cancel_requested"] = True
        job["status"] = "cancelling"
        self.save(job)
        if job["session_id"]:
            await self.opencode.interrupt(job["session_id"])
            await self.refresh(task_id)
        return self.public(self.get(task_id))

    async def reply(self, task_id, request_id, approve):
        async with self.refresh_locks.setdefault(task_id, asyncio.Lock()):
            job = await self._refresh(task_id)
            if request_id not in {r["id"] for r in job.get("permission_requests", [])}:
                raise ValueError("Permission is not pending for this task")
            await self.opencode.permission_reply(job["session_id"], request_id, approve)
            current = self.get(task_id)
            if current["status"] == "waiting_permission":
                current["permission_requests"] = [
                    request
                    for request in current.get("permission_requests", [])
                    if request["id"] != request_id
                ]
                if not current["permission_requests"]:
                    now = time.time()
                    finish_permission_wait(current, now)
                    current["elapsed_seconds"] = round(active_elapsed(current, now), 2)
                    current["status"] = "running"
                self.save(current)
        return {"task_id": task_id, "approved_once": approve}

    def retained_worktree(self, job):
        copied = job.get("workspace", {})
        if not copied.get("worktree") or not job.get("artifacts"):
            raise ValueError("Task has no retained worktree and exported patch")
        directory = Path(copied["worktree"])
        expected = self.state / "jobs" / job["id"] / "worktree"
        if directory.resolve() != expected.resolve():
            raise ValueError("Worktree ownership check failed")
        if not directory.is_dir() or not Path(job["artifacts"]["patch"]).is_file():
            raise ValueError("Retained worktree or exported patch is missing")
        return directory

    async def confirm_stopped(self, job):
        if job.get("dispatch", {}).get("status") in {"pending", "uncertain"}:
            raise ValueError("Prompt admission is still uncertain")
        if session := job.get("session_id"):
            active = (await self.opencode.request("GET", "/api/session/active"))["data"]
            inbox = await self.opencode.inbox(session)
            if session in active or inbox:
                raise ValueError("Session still has active execution or queued input")

    async def followup(self, task_id, request_id, instruction, recovery_reason=None):
        async with self.lock:
            job = await self.refresh(task_id)
            previous = next(
                (item for item in job.get("followups", []) if item["request_id"] == request_id), None
            )
            if previous:
                if (
                    previous["instruction"] != instruction
                    or previous.get("recovery_reason") != recovery_reason
                ):
                    raise ValueError("request_id already belongs to a different follow-up")
                return self.public(job)
            if job["status"] not in {"completed", "failed"} or job.get("cleaned_up"):
                raise ValueError("Follow-up requires a completed or failed task with a retained worktree")
            if job.get("verification", {}).get("status") == "running":
                raise ValueError("Verification is still running or its completion is uncertain")
            self.retained_worktree(job)
            if job["status"] == "failed":
                if not recovery_reason or not recovery_reason.strip():
                    raise ValueError("Failed tasks require recovery_reason explaining the addressed cause")
                if not job.get("session_id") or job.get("session_confirmed") is False:
                    raise ValueError("Failed task has no confirmed session; start a new task")
                if job.get("dispatch", {}).get("status") != "accepted":
                    raise ValueError("Failed task has no confirmed prompt admission; start a new task")
                await self.confirm_stopped(job)
                assistants = [
                    message
                    for message in await self.opencode.messages(job["session_id"])
                    if message.get("type") == "assistant"
                ]
                backend = self.config["backends"][job["backend"]]
                if any(
                    message.get("model", {}).get("providerID") != backend["provider"]
                    or message.get("model", {}).get("id") != backend["model"]
                    for message in assistants
                ):
                    raise ValueError("Session used an unexpected model; start a new task")
            if not await self.health[job["backend"]].probe(
                self.client, self.config["health_failure_threshold"]
            ):
                raise ValueError("Backend is not reachable")
            if not self.status()[job["backend"]]["free"]:
                raise ValueError("Backend has no free slots")
            self.archive_run(job)
            job["run"] = job.get("run", 1) + 1
            job["status"] = "running"
            job["submitted_at"] = time.time()
            job["turns_used"] = 0
            job["turn_limit"] = self.config["max_assistant_turns"]
            job["final_turn_grace"] = 1
            job["elapsed_seconds"] = 0
            job["time_limit_seconds"] = self.config["task_timeout_seconds"]
            job.setdefault("followups", []).append(
                {"request_id": request_id, "instruction": instruction, "recovery_reason": recovery_reason}
            )
            job.pop("finished_at", None)
            job.pop("feedback", None)
            job.pop("verification", None)
            for key in [
                "summary",
                "progress",
                "activity",
                "evidence",
                "artifacts",
                "duration_seconds",
                "permission_requests",
                "error",
                "opencode_error",
                "connection_error",
                "stop_reason",
                "cancel_requested",
                "permission_wait_started_at",
                "permission_wait_seconds",
            ]:
                job.pop(key, None)
            job["dispatch"] = {"id": "msg_" + uuid.uuid4().hex, "status": "pending"}
            self.save(job)
            await self.send_prompt(
                task_id,
                "Follow-up within the original allowed_paths and assignment constraints:\n"
                + instruction
                + ("\nRecovery reason: " + recovery_reason if recovery_reason else "")
                + "\n"
                + run_guidance(job["turn_limit"]),
                dispatch_prepared=True,
            )
        return self.public(self.get(task_id))

    async def verify(self, task_id, request_id, checks):
        parsed = [Check.model_validate(check) for check in checks]
        specifications = [check.model_dump() for check in parsed]
        async with self.lock:
            job = self.get(task_id)
            if job["status"] not in {"completed", "failed"} or job.get("cleaned_up"):
                raise ValueError("Verification requires a completed or failed task with a retained worktree")
            previous = job.get("verification", {})
            if previous.get("request_id") == request_id:
                if previous["specifications"] != specifications:
                    raise ValueError("request_id already belongs to different checks")
                return previous
            if previous.get("status") == "running":
                raise ValueError("Previous verification is still running or its completion is uncertain")
            directory = self.retained_worktree(job)
            if job["status"] == "failed":
                await self.confirm_stopped(job)
            logs = directory.parent / "checks" / uuid.uuid4().hex
            logs.mkdir(parents=True)
            report = {
                "request_id": request_id,
                "specifications": specifications,
                "status": "running",
                "checks": [],
                "log_directory": str(logs),
            }
            job["verification"] = report
            job.pop("feedback", None)
            self.save(job)
            deadline = time.monotonic() + 30
            try:
                for index, check in enumerate(parsed):
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        report["checks"].append(
                            {
                                "argv": check.argv,
                                "exit_code": None,
                                "timed_out": True,
                                "error": "Verification budget exhausted",
                            }
                        )
                        break
                    report["checks"].append(
                        await run_check(
                            check, directory, logs / f"{index}.log", min(remaining, check.timeout_seconds)
                        )
                    )
                report["status"] = (
                    "passed"
                    if all(check["exit_code"] == 0 and not check["timed_out"] for check in report["checks"])
                    else "failed"
                )
                exported = await asyncio.to_thread(workspace.export_changes, job, directory.parent)
                if exported["result_tree"] != job["artifacts"]["result_tree"]:
                    report["status"] = "failed"
                    report["error"] = "Checks changed the exported source tree; review the updated patch"
                job["artifacts"] = exported
                if exported["scope_violations"]:
                    job["status"] = "failed"
                    report["status"] = "failed"
                    report["error"] = "Checks changed files outside allowed_paths"
            except asyncio.CancelledError:
                report["status"] = "cancelled"
                raise
            except (OSError, ValueError) as error:
                report["status"] = "failed"
                report["error"] = str(error)
            finally:
                job["verification"] = report
                self.save(job)
            return report

    def feedback(self, task_id, verdict, rework_minutes, cloud_input_tokens, cloud_output_tokens, notes):
        job = self.get(task_id)
        if job["status"] not in TERMINAL:
            raise ValueError("Feedback requires a finished task")
        verification = job.get("verification", {}).get("status")
        if verification == "running" or (verdict == "accepted" and verification in {"failed", "cancelled"}):
            raise ValueError("Resolve incomplete or failing verification before accepting this task")
        job["feedback"] = {
            "verdict": verdict,
            "rework_minutes": rework_minutes,
            "notes": notes,
            "coordinator_input_tokens": cloud_input_tokens,
            "coordinator_output_tokens": cloud_output_tokens,
            "source": "coordinator_reported",
            "recorded_at": time.time(),
        }
        self.save(job)
        return job["feedback"]

    async def cleanup(self, task_id):
        async with self.lock:
            return await self._cleanup(task_id)

    async def _cleanup(self, task_id):
        job = self.get(task_id)
        if job.get("verification", {}).get("status") == "running":
            raise ValueError("Verification is still running or its completion is uncertain")
        if (
            job["status"] not in TERMINAL
            or job.get("feedback", {}).get("verdict") not in {"accepted", "rejected"}
            or not job.get("artifacts")
        ):
            raise ValueError(
                "Cleanup requires a finished task, exported patch and accepted/rejected feedback"
            )
        if job.get("cleaned_up"):
            return {"cleaned_up": True}
        path = Path(job["workspace"]["worktree"])
        if path.resolve() != (self.state / "jobs" / task_id / "worktree").resolve():
            raise ValueError("Worktree ownership check failed")
        repository = Path(job["workspace"]["repository"])
        if not repository.is_dir():
            raise ValueError("Source repository is unavailable; restore it before cleanup")
        if path.exists():
            current = await asyncio.to_thread(
                workspace.tree_from_working_copy, path, job["workspace"]["snapshot_commit"], path.parent
            )
            if current != job["artifacts"]["result_tree"]:
                raise ValueError("Worktree changed after export; preserve its new changes before cleanup")
        await asyncio.to_thread(workspace.git, repository, "worktree", "remove", "--force", str(path))
        job["cleaned_up"] = True
        self.save(job)
        return {"cleaned_up": True, "retained_patch": job["artifacts"]["patch"]}

    def metrics(self):
        jobs = self.jobs()

        def total(values):
            known = [value for value in values if value is not None]
            return sum(known) if known else None

        return {
            "tasks": len(jobs),
            "states": dict(Counter(j["status"] for j in jobs)),
            "feedback": dict(Counter(j["feedback"]["verdict"] for j in jobs if j.get("feedback"))),
            "duration_seconds": sum(j.get("duration_seconds", 0) for j in jobs),
            "rework_minutes": sum(j.get("feedback", {}).get("rework_minutes", 0) or 0 for j in jobs),
            "worker_tokens": {
                k: total((j.get("tokens") or {}).get(k) for j in jobs)
                for k in ["input", "output", "reasoning"]
            },
            "worker_usage_samples": sum(j.get("tokens") is not None for j in jobs),
            "coordinator_tokens": {
                k: total(j.get("feedback", {}).get(k) for j in jobs)
                for k in ["coordinator_input_tokens", "coordinator_output_tokens"]
            },
            "coordinator_usage_samples": sum(
                j.get("feedback", {}).get("coordinator_input_tokens") is not None for j in jobs
            ),
            "savings": "Not inferred: requires measured coordinator usage and a comparable non-delegated baseline",
        }

    def public(self, job):
        keys = [
            "id",
            "backend",
            "status",
            "goal",
            "created_at",
            "duration_seconds",
            "session_id",
            "workspace",
            "artifacts",
            "summary",
            "progress",
            "activity",
            "evidence",
            "turns_used",
            "turn_limit",
            "final_turn_grace",
            "elapsed_seconds",
            "time_limit_seconds",
            "tokens",
            "permission_requests",
            "error",
            "opencode_error",
            "connection_error",
            "stop_reason",
            "feedback",
            "verification",
            "dispatch",
            "cleaned_up",
        ]
        result = {
            "run": job.get("run", 1),
            "usage_scope": "session_cumulative",
            "patch_scope": "task_input_snapshot",
            **{key: job[key] for key in keys if key in job},
        }
        evidence = self.state / "jobs" / job["id"] / "opencode.json"
        if job["status"] in TERMINAL and "evidence" not in result and evidence.is_file():
            result["evidence"] = str(evidence)
        return result

    async def recover(self):
        for job in self.jobs():
            if job["status"] in ACTIVE:
                if job.get("dispatch", {}).get("status") == "pending":
                    job["dispatch"]["status"] = "uncertain"
                    job["status"] = "cancelling"
                    job["stop_reason"] = "Scheduler restarted before prompt admission was confirmed"
                    self.save(job)
                if job["status"] == "preparing":
                    path = self.state / "jobs" / job["id"] / "worktree"
                    if path.exists() and not job.get("workspace"):
                        job["workspace"] = {
                            "repository": job["repository"],
                            "worktree": str(path),
                            "snapshot_commit": workspace.git(path, "rev-parse", "HEAD").decode().strip(),
                        }
                    if job["session_id"]:
                        job["status"] = "cancelling"
                        job["stop_reason"] = "Scheduler restarted during preparation"
                        self.save(job)
                        try:
                            await self.cancel(job["id"])
                        except httpx.HTTPStatusError as error:
                            if error.response.status_code == 404 and not job.get("session_confirmed"):
                                await self.finalize(job, "failed")
                            else:
                                LOG.exception("Could not reconcile preparing session")
                        continue
                if job["session_id"]:
                    await self.refresh(job["id"])
                else:
                    job["error"] = (
                        "Service restarted before a session was recorded; inspect retained preparation artifacts"
                    )
                    await self.finalize(job, "failed")
