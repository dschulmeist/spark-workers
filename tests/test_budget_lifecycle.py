import asyncio
import copy
import json
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx

from spark_workers.manager import Manager

CONFIG = {
    "task_timeout_seconds": 60,
    "max_assistant_turns": 24,
    "backends": {"test": {"base_url": "http://test/v1", "provider": "p", "model": "m", "capacity": 1}},
}


class BudgetLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.client = httpx.AsyncClient()
        self.addAsyncCleanup(self.client.aclose)
        self.manager = Manager(self.temporary.name, copy.deepcopy(CONFIG), self.client)
        self.addCleanup(self.manager.db.close)
        self.api = SimpleNamespace(
            info=AsyncMock(return_value={"time": {"updated": time.time() * 1000}}),
            messages=AsyncMock(return_value=[]),
            request=AsyncMock(return_value={"data": {"session": {}}}),
            permissions=AsyncMock(return_value=[]),
            permission_reply=AsyncMock(),
            interrupt=AsyncMock(),
        )
        self.manager.opencode = self.api
        (self.manager.state / "jobs/task").mkdir(parents=True)
        now = time.time()
        self.manager.save(
            {
                "id": "task",
                "request_id": "initial",
                "backend": "test",
                "status": "running",
                "session_id": "session",
                "created_at": now - 1,
                "submitted_at": now - 1,
                "allowed_paths": [],
            }
        )

    def assistant(self, finish, text=""):
        return {
            "type": "assistant",
            "model": {"providerID": "p", "id": "m"},
            "time": {"created": time.time() * 1000},
            "finish": finish,
            "content": [
                {"type": "text", "text": text},
                {
                    "type": "tool",
                    "id": "tool_read",
                    "name": "read",
                    "state": {"status": "completed", "output": "private"},
                },
            ],
        }

    async def test_read_only_run_uses_configured_limit_and_exposes_progress(self):
        self.api.messages.return_value = [self.assistant("tool-calls", "Checking the configuration.")] * 7
        result = await self.manager.result("task")
        self.assertEqual(result["status"], "running")
        self.assertEqual((result["turns_used"], result["turn_limit"]), (7, 24))
        self.assertEqual(result["final_turn_grace"], 1)
        self.assertEqual(result["progress"], "Checking the configuration.")
        self.assertEqual(result["activity"], {"tool": "read", "status": "completed"})
        self.assertTrue(Path(result["evidence"]).is_file())
        self.assertEqual(
            json.loads(Path(result["evidence"]).read_text())["messages"][-1]["type"], "assistant"
        )
        self.assertFalse(Path(result["evidence"]).with_name("opencode.json.tmp").exists())
        self.assertNotIn("private", str(result))
        waited = await self.manager.wait(["task"], "all", 0)
        self.assertEqual(waited["tasks"][0]["turns_used"], 7)
        self.assertEqual(waited["tasks"][0]["final_turn_grace"], 1)
        self.assertEqual(waited["tasks"][0]["progress"], "Checking the configuration.")
        self.api.interrupt.assert_not_awaited()

    async def test_final_response_at_turn_limit_can_finish(self):
        self.manager.config["max_assistant_turns"] = 2
        first = self.assistant("tool-calls")
        final = self.assistant(None)
        self.api.messages.return_value = [first, final]
        self.assertEqual((await self.manager.refresh("task"))["status"], "running")
        self.api.interrupt.assert_not_awaited()

        final["finish"] = "stop"
        final["content"] = [{"type": "text", "text": "Finished findings."}]
        self.api.info.return_value = {"time": {"idle": time.time() * 1000}, "outcome": "succeeded"}
        self.api.request.return_value = {"data": {}}
        result = await self.manager.refresh("task")
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["summary"], "Finished findings.")
        self.api.interrupt.assert_not_awaited()

    async def test_one_final_response_is_allowed_after_work_budget(self):
        self.manager.config["max_assistant_turns"] = 2
        first = self.assistant("tool-calls")
        second = self.assistant("tool-calls")
        self.api.messages.return_value = [first, second]
        self.assertEqual((await self.manager.refresh("task"))["status"], "running")
        self.api.interrupt.assert_not_awaited()

        final = self.assistant("stop", "Finished after the work budget.")
        self.api.messages.return_value = [first, second, final]
        self.api.info.return_value = {"time": {"idle": time.time() * 1000}, "outcome": "succeeded"}
        self.api.request.return_value = {"data": {}}
        result = await self.manager.refresh("task")
        self.assertEqual(result["status"], "completed")
        self.assertEqual((result["turns_used"], result["turn_limit"], result["final_turn_grace"]), (3, 2, 1))
        self.api.interrupt.assert_not_awaited()

    async def test_extra_tool_turn_is_not_hidden_by_a_later_final_response(self):
        self.manager.config["max_assistant_turns"] = 2
        self.api.messages.return_value = [self.assistant("tool-calls")] * 3 + [
            self.assistant("stop", "Findings from the completed work.")
        ]
        self.api.info.return_value = {"time": {"idle": time.time() * 1000}, "outcome": "succeeded"}
        self.api.request.return_value = {"data": {}}
        result = await self.manager.refresh("task")
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["error"], "Worker turn limit reached")
        self.assertEqual(result["summary"], "Findings from the completed work.")

    async def test_followup_turns_do_not_include_previous_runs(self):
        now = time.time()
        job = self.manager.get("task")
        job.update(run=2, submitted_at=now, turn_limit=2)
        self.manager.save(job)
        old = self.assistant("tool-calls")
        old["time"]["created"] = (now - 10) * 1000
        current = self.assistant("tool-calls")
        current["time"]["created"] = (now + 0.01) * 1000
        self.api.messages.return_value = [old] * 24 + [current]
        result = await self.manager.refresh("task")
        self.assertEqual(result["status"], "running")
        self.assertEqual(result["turns_used"], 1)
        self.api.interrupt.assert_not_awaited()

    async def test_late_poll_does_not_fail_a_result_finished_within_budget(self):
        job = self.manager.get("task")
        job.update(created_at=100, submitted_at=100, time_limit_seconds=60)
        self.manager.save(job)
        final = self.assistant("stop", "Finished findings.")
        final["time"] = {"created": 120000, "completed": 150000}
        self.api.messages.return_value = [final]
        self.api.info.return_value = {"time": {"idle": 150000}, "outcome": "succeeded"}
        self.api.request.return_value = {"data": {}}
        with patch("spark_workers.manager.time.time", return_value=200):
            result = await self.manager.refresh("task")
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["elapsed_seconds"], 50)
        self.api.interrupt.assert_not_awaited()

    async def test_permission_wait_does_not_consume_run_time(self):
        job = self.manager.get("task")
        job.update(created_at=100, submitted_at=100, time_limit_seconds=60)
        self.manager.save(job)
        self.api.permissions.return_value = [{"id": "permission"}]
        with patch("spark_workers.manager.time.time", return_value=150):
            self.assertEqual((await self.manager.refresh("task"))["status"], "waiting_permission")
        with patch("spark_workers.manager.time.time", return_value=650):
            waiting = await self.manager.refresh("task")
        self.assertEqual(waiting["status"], "waiting_permission")
        self.assertEqual(waiting["elapsed_seconds"], 50)
        self.assertNotIn("stop_reason", waiting)
        with patch("spark_workers.manager.time.time", return_value=660):
            await self.manager.reply("task", "permission", True)
        resumed = self.manager.get("task")
        self.assertEqual(resumed["status"], "running")
        self.assertEqual(resumed["elapsed_seconds"], 50)
        self.assertEqual(resumed["permission_requests"], [])

    async def test_pending_permission_is_not_discarded_by_a_late_first_poll(self):
        job = self.manager.get("task")
        job.update(created_at=100, submitted_at=100, time_limit_seconds=60)
        self.manager.save(job)
        self.api.permissions.return_value = [{"id": "permission"}]
        with patch("spark_workers.manager.time.time", return_value=200):
            result = await self.manager.refresh("task")
        self.assertEqual(result["status"], "waiting_permission")
        self.assertNotIn("stop_reason", result)
        self.assertEqual(result["permission_requests"][0]["id"], "permission")
        self.api.interrupt.assert_not_awaited()

    async def test_refresh_waits_until_permission_reply_updates_the_wait_time(self):
        self.api.permissions.return_value = [{"id": "permission"}]
        await self.manager.refresh("task")
        entered = asyncio.Event()
        release = asyncio.Event()

        async def reply(session, request, approve):
            entered.set()
            await release.wait()
            self.api.permissions.return_value = []

        self.api.permission_reply.side_effect = reply
        reply_task = asyncio.create_task(self.manager.reply("task", "permission", True))
        await entered.wait()
        refresh_task = asyncio.create_task(self.manager.refresh("task"))
        await asyncio.sleep(0)
        self.assertFalse(refresh_task.done())
        release.set()
        await reply_task
        result = await refresh_task
        self.assertEqual(result["status"], "running")
        self.assertNotIn("permission_wait_started_at", result)
        self.assertGreaterEqual(result["permission_wait_seconds"], 0)

    async def test_manual_cancel_is_not_reclassified_when_budget_is_exhausted(self):
        job = self.manager.get("task")
        job.update(status="cancelling", cancel_requested=True, created_at=100, submitted_at=100)
        self.manager.save(job)
        aborted = self.assistant("error")
        aborted["time"]["completed"] = time.time() * 1000
        aborted["error"] = {"type": "aborted", "message": "Step interrupted"}
        self.api.messages.return_value = [aborted]
        self.api.info.return_value = {"time": {}, "outcome": "failed"}
        self.api.request.return_value = {"data": {}}
        result = await self.manager.refresh("task")
        self.assertEqual(result["status"], "cancelled")
        self.assertNotIn("stop_reason", result)

    async def test_cancel_during_permission_poll_is_preserved(self):
        async def cancel_during_poll(session):
            job = self.manager.get("task")
            job.update(status="cancelling", cancel_requested=True)
            self.manager.save(job)
            return []

        self.api.permissions.side_effect = cancel_during_poll
        result = await self.manager.refresh("task")
        self.assertEqual(result["status"], "cancelling")
        self.assertTrue(self.manager.get("task")["cancel_requested"])

    async def test_cancel_during_message_fetch_wins_over_expired_budget(self):
        job = self.manager.get("task")
        job.update(created_at=100, submitted_at=100)
        self.manager.save(job)

        async def cancel_during_fetch(session):
            current = self.manager.get("task")
            current.update(status="cancelling", cancel_requested=True)
            self.manager.save(current)
            return []

        self.api.messages.side_effect = cancel_during_fetch
        self.api.info.return_value = {"time": {}, "outcome": "failed"}
        self.api.request.return_value = {"data": {}}
        result = await self.manager.refresh("task")
        self.assertEqual(result["status"], "cancelled")
        self.assertNotIn("stop_reason", result)
