import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx

from spark_workers import workspace
from spark_workers.manager import Manager

CONFIG = {
    "task_timeout_seconds": 1200,
    "max_assistant_turns": 24,
    "backends": {"test": {"base_url": "http://test/v1", "provider": "p", "model": "m", "capacity": 1}},
}


class DenialTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.client = httpx.AsyncClient()
        self.addAsyncCleanup(self.client.aclose)
        self.manager = Manager(self.temporary.name, CONFIG, self.client)
        self.addCleanup(self.manager.db.close)
        self.manager.health["test"].available = True
        self.submitted = time.time() - 1
        self.error_response = {
            "type": "assistant",
            "model": {"providerID": "p", "id": "m"},
            "time": {"created": self.submitted * 1000 + 1, "completed": time.time() * 1000},
            "finish": "error",
            "error": {"type": "aborted", "message": "Step interrupted"},
            "content": [],
        }
        self.api = SimpleNamespace(
            info=AsyncMock(return_value={"time": {"updated": time.time() * 1000}}),
            messages=AsyncMock(return_value=[self.error_response]),
            inbox=AsyncMock(return_value=[]),
            request=AsyncMock(return_value={"data": {}}),
            permissions=AsyncMock(return_value=[]),
            permission_reply=AsyncMock(),
            prompt=AsyncMock(),
        )
        self.manager.opencode = self.api
        (self.manager.state / "jobs/task").mkdir(parents=True)
        self.manager.save(
            {
                "id": "task",
                "request_id": "denial",
                "backend": "test",
                "status": "running",
                "session_id": "session",
                "created_at": time.time(),
                "submitted_at": self.submitted,
                "dispatch": {"id": "input", "status": "accepted"},
            }
        )

    async def assert_slot_retained(self):
        result = await self.manager.refresh("task")
        self.assertEqual(result["status"], "running")
        self.assertEqual(self.manager.status()["test"]["free"], 0)
        self.assertNotIn("artifacts", result)
        self.api.prompt.assert_not_awaited()

    async def test_denied_command_without_idle_finalizes_failure_and_exports_patch(self):
        repo = Path(self.temporary.name) / "repo"
        repo.mkdir()
        for args in [
            ("init", "-q"),
            ("config", "user.name", "Test User"),
            ("config", "user.email", "test@example.test"),
        ]:
            workspace.git(repo, *args)
        (repo / "code.py").write_text("value = 1\n")
        workspace.git(repo, "add", ".")
        workspace.git(repo, "commit", "-qm", "Initial")
        job = self.manager.get("task")
        job["workspace"] = workspace.prepare(
            repo,
            workspace.git(repo, "rev-parse", "HEAD").decode().strip(),
            False,
            self.manager.state / "jobs/task/worktree",
            self.manager.state,
        )
        job["allowed_paths"] = ["code.py"]
        self.manager.save(job)
        (Path(job["workspace"]["worktree"]) / "code.py").write_text("value = 2\n")
        self.api.messages.return_value = []
        self.api.permissions.return_value = [{"id": "permission"}]

        async def reject(session, request, approve):
            self.api.messages.return_value = [self.error_response]
            self.api.permissions.return_value = []

        self.api.permission_reply.side_effect = reject
        await self.manager.reply("task", "permission", False)
        result = await self.manager.refresh("task")
        self.api.permission_reply.assert_awaited_once_with("session", "permission", False)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["error"], "Step interrupted")
        self.assertEqual(result["opencode_error"], {"type": "aborted", "message": "Step interrupted"})
        self.assertEqual(self.manager.status()["test"]["free"], 1)
        self.assertEqual(result["artifacts"]["changed_files"], ["code.py"])
        self.assertIn("+value = 2", Path(result["artifacts"]["patch"]).read_text())
        self.api.prompt.assert_not_awaited()

    async def test_failed_session_preserves_specific_error(self):
        self.api.info.return_value["outcome"] = "failed"
        self.error_response["error"] = {"type": "api", "message": "Model server unavailable", "status": 503}
        result = await self.manager.result("task")
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["error"], "Model server unavailable")
        self.assertEqual(result["opencode_error"]["status"], 503)

    async def test_error_preview_is_bounded_and_excludes_response_body(self):
        self.error_response["error"] = {
            "type": "api",
            "message": "x" * 5000,
            "response": {"body": "large response body"},
        }
        result = await self.manager.result("task")
        self.assertEqual(len(result["error"]), 1200)
        self.assertEqual(len(result["opencode_error"]["message"]), 1200)
        self.assertNotIn("response", result["opencode_error"])

    async def test_previous_run_error_is_not_reported_as_current_failure(self):
        self.error_response["time"]["created"] = (self.submitted - 10) * 1000
        self.api.info.return_value["time"]["idle"] = time.time() * 1000
        result = await self.manager.result("task")
        self.assertEqual(result["error"], "Session ended without a final assistant response")
        self.assertNotIn("opencode_error", result)

    async def test_active_session_retains_slot_after_error(self):
        self.api.request.return_value = {"data": {"session": {}}}
        await self.assert_slot_retained()

    async def test_queued_input_retains_slot_after_error(self):
        self.api.inbox.return_value = [{"id": "input"}]
        await self.assert_slot_retained()

    async def test_previous_run_error_retains_slot(self):
        self.error_response["time"]["created"] = (self.submitted - 10) * 1000
        await self.assert_slot_retained()

    async def test_unfinished_error_response_retains_slot(self):
        del self.error_response["time"]["completed"]
        await self.assert_slot_retained()

    async def test_newer_unfinished_response_retains_slot(self):
        self.api.messages.return_value = [
            self.error_response,
            {
                "type": "assistant",
                "model": {"providerID": "p", "id": "m"},
                "time": {"created": time.time() * 1000},
                "content": [],
            },
        ]
        await self.assert_slot_retained()

    async def test_missing_explicit_error_retains_slot(self):
        del self.error_response["error"]
        await self.assert_slot_retained()
