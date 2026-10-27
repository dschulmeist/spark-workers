import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx

from spark_workers import workspace
from spark_workers.manager import Manager

CONFIG = {
    "health_failure_threshold": 1,
    "task_timeout_seconds": 1200,
    "max_assistant_turns": 24,
    "backends": {"test": {"base_url": "http://test/v1", "provider": "p", "model": "m", "capacity": 1}},
}


class RecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        self.repo = root / "repo"
        self.repo.mkdir()
        for args in [
            ("init", "-q"),
            ("config", "user.name", "Test User"),
            ("config", "user.email", "test@example.test"),
        ]:
            workspace.git(self.repo, *args)
        (self.repo / "code.py").write_text("value = 1\n")
        workspace.git(self.repo, "add", ".")
        workspace.git(self.repo, "commit", "-qm", "Initial")
        client = httpx.AsyncClient(
            transport=httpx.MockTransport(lambda r: httpx.Response(200, json={"data": [{"id": "m"}]}))
        )
        self.addAsyncCleanup(client.aclose)
        self.manager = Manager(root, CONFIG, client)
        self.addCleanup(self.manager.db.close)
        self.api = SimpleNamespace(
            info=AsyncMock(return_value={"time": {"updated": time.time() * 1000}}),
            request=AsyncMock(return_value={"data": {}}),
            inbox=AsyncMock(return_value=[]),
            messages=AsyncMock(return_value=[]),
            permissions=AsyncMock(return_value=[]),
            prompt=AsyncMock(),
        )
        self.manager.opencode = self.api
        folder = self.manager.state / "jobs/task"
        folder.mkdir(parents=True)
        copied = workspace.prepare(
            self.repo,
            workspace.git(self.repo, "rev-parse", "HEAD").decode().strip(),
            False,
            folder / "worktree",
            folder,
        )
        self.directory = Path(copied["worktree"])
        (self.directory / "code.py").write_text("value = 2\n")
        job = {
            "id": "task",
            "request_id": "initial",
            "backend": "test",
            "status": "failed",
            "repository": str(self.repo),
            "session_id": "session",
            "session_confirmed": True,
            "dispatch": {"id": "msg_initial", "status": "accepted"},
            "created_at": time.time(),
            "finished_at": time.time(),
            "workspace": copied,
            "allowed_paths": ["code.py"],
            "error": "Step interrupted",
            "opencode_error": {"type": "aborted", "message": "Step interrupted"},
        }
        job["artifacts"] = workspace.export_changes(job, folder)
        self.manager.save(job)
        self.checks = [{"argv": [sys.executable, "-B", "-c", "from code import value; assert value == 2"]}]

    async def test_failed_patch_can_be_verified_without_marking_run_completed(self):
        report = await self.manager.verify("task", "check", self.checks)
        self.assertEqual(report["status"], "passed")
        self.assertEqual(self.manager.get("task")["status"], "failed")
        self.api.prompt.assert_not_awaited()
        self.assertEqual((self.repo / "code.py").read_text(), "value = 1\n")

    async def test_explicit_recovery_preserves_failure_and_verification_in_history(self):
        await self.manager.verify("task", "check", self.checks)
        reason = "Use a read-only check instead of the rejected shell command"
        result = await self.manager.followup("task", "fix", "Review the retained patch", reason)
        self.assertEqual(result["status"], "running")
        self.assertEqual(result["run"], 2)
        self.assertNotIn("opencode_error", result)
        self.assertNotIn("error", result)
        previous = await self.manager.result("task", 1)
        self.assertEqual(previous["status"], "failed")
        self.assertEqual(previous["error"], "Step interrupted")
        self.assertEqual(previous["verification"]["status"], "passed")
        self.assertIn("+value = 2", Path(previous["artifacts"]["patch"]).read_text())
        self.assertEqual(self.manager.get("task")["allowed_paths"], ["code.py"])
        self.assertIn(reason, self.api.prompt.call_args.args[1])
        await self.manager.followup("task", "fix", "Review the retained patch", reason)
        self.api.prompt.assert_awaited_once()
        with self.assertRaisesRegex(ValueError, "different follow-up"):
            await self.manager.followup("task", "fix", "Review the retained patch", "Another cause")

    async def test_failed_followup_requires_recovery_reason(self):
        for reason in [None, "   "]:
            with self.subTest(reason=reason), self.assertRaisesRegex(ValueError, "recovery_reason"):
                await self.manager.followup("task", "fix", "Review retained changes", reason)
        self.api.prompt.assert_not_awaited()

    async def test_restart_before_followup_dispatch_does_not_reuse_old_admission(self):
        with patch.object(self.manager, "send_prompt", new=AsyncMock(side_effect=RuntimeError("restart"))):
            with self.assertRaisesRegex(RuntimeError, "restart"):
                await self.manager.followup("task", "fix", "Review retained changes", "Cause addressed")
        pending = self.manager.get("task")
        self.assertEqual(pending["dispatch"]["status"], "pending")
        self.assertNotEqual(pending["dispatch"]["id"], "msg_initial")

        await self.manager.recover()
        recovered = self.manager.get("task")
        self.assertEqual(recovered["status"], "cancelling")
        self.assertEqual(recovered["dispatch"]["status"], "uncertain")
        self.api.prompt.assert_not_awaited()
        self.assertEqual((await self.manager.result("task", 1))["status"], "failed")

    async def test_active_queued_or_uncertain_execution_blocks_failed_lifecycle(self):
        for condition in ["active", "queued", "uncertain"]:
            with self.subTest(condition=condition):
                self.api.request.return_value = {"data": {"session": {}} if condition == "active" else {}}
                self.api.inbox.return_value = [{"id": "msg_pending"}] if condition == "queued" else []
                job = self.manager.get("task")
                job["dispatch"]["status"] = "uncertain" if condition == "uncertain" else "accepted"
                self.manager.save(job)
                with self.assertRaises(ValueError):
                    await self.manager.verify("task", "check", self.checks)
                with self.assertRaises(ValueError):
                    await self.manager.followup("task", "fix", "Review retained changes", "Cause addressed")
        self.api.prompt.assert_not_awaited()

    async def test_failed_followup_gets_a_fresh_budget_and_keeps_prior_failure(self):
        job = self.manager.get("task")
        job["created_at"] = time.time() - 1300
        job["submitted_at"] = time.time() - 1300
        job["stop_reason"] = "Worker turn limit reached"
        job["turns_used"] = 24
        job["turn_limit"] = 24
        self.manager.save(job)
        assistant = {"type": "assistant", "model": {"providerID": "p", "id": "m"}}
        self.api.messages.return_value = [assistant] * 24

        result = await self.manager.followup("task", "fix", "Summarize retained findings", "Budget exhausted")
        self.assertEqual(result["status"], "running")
        self.assertEqual(result["run"], 2)
        self.assertEqual(result["turns_used"], 0)
        self.assertEqual(result["turn_limit"], CONFIG["max_assistant_turns"])
        self.assertEqual(result["elapsed_seconds"], 0)
        previous = await self.manager.result("task", 1)
        self.assertEqual(previous["status"], "failed")
        self.assertEqual(previous["stop_reason"], "Worker turn limit reached")
        self.api.prompt.assert_awaited_once()

    async def test_failed_followup_rejects_an_unexpected_session_model(self):
        self.api.messages.return_value = [
            {"type": "assistant", "model": {"providerID": "p", "id": "unexpected"}}
        ]
        with self.assertRaisesRegex(ValueError, "unexpected model"):
            await self.manager.followup("task", "fix", "Review retained changes", "Cause addressed")
        self.api.prompt.assert_not_awaited()

    async def test_scope_violations_still_fail_independent_verification(self):
        (self.directory / "outside.txt").write_text("not permitted")
        job = self.manager.get("task")
        job["artifacts"] = workspace.export_changes(job, self.directory.parent)
        self.manager.save(job)
        report = await self.manager.verify("task", "check", self.checks)
        self.assertEqual(report["status"], "failed")
        self.assertEqual(self.manager.get("task")["status"], "failed")
        with self.assertRaises(ValueError):
            self.manager.feedback("task", "accepted", 0, None, None, "")

    async def test_uncertain_verification_and_cleaned_worktrees_cannot_resume(self):
        job = self.manager.get("task")
        job["verification"] = {"status": "running"}
        self.manager.save(job)
        with self.assertRaisesRegex(ValueError, "uncertain"):
            await self.manager.followup("task", "fix", "Review retained changes", "Cause addressed")
        with self.assertRaisesRegex(ValueError, "uncertain"):
            await self.manager.verify("task", "check", self.checks)
        job.pop("verification")
        job["cleaned_up"] = True
        self.manager.save(job)
        with self.assertRaises(ValueError):
            await self.manager.followup("task", "fix", "Review retained changes", "Cause addressed")
        with self.assertRaises(ValueError):
            await self.manager.verify("task", "check", self.checks)

    async def test_failed_followup_still_requires_free_capacity(self):
        self.manager.save(
            {"id": "busy", "request_id": "busy", "backend": "test", "status": "running", "session_id": None}
        )
        with self.assertRaisesRegex(ValueError, "free slots"):
            await self.manager.followup("task", "fix", "Review retained changes", "Cause addressed")
        self.api.prompt.assert_not_awaited()

    async def test_missing_patch_or_foreign_worktree_blocks_failed_lifecycle(self):
        job = self.manager.get("task")
        job["workspace"]["worktree"] = str(self.repo)
        self.manager.save(job)
        with self.assertRaisesRegex(ValueError, "ownership"):
            await self.manager.verify("task", "check", self.checks)
        with self.assertRaisesRegex(ValueError, "ownership"):
            await self.manager.followup("task", "fix", "Review retained changes", "Cause addressed")
        job["workspace"]["worktree"] = str(self.directory)
        self.manager.save(job)
        patch = Path(job["artifacts"]["patch"])
        patch.rename(patch.with_suffix(".saved"))
        with self.assertRaisesRegex(ValueError, "missing"):
            await self.manager.verify("task", "check", self.checks)
        with self.assertRaisesRegex(ValueError, "missing"):
            await self.manager.followup("task", "fix", "Review retained changes", "Cause addressed")
        self.api.prompt.assert_not_awaited()

    async def test_unreachable_session_metadata_blocks_checks_and_recovery(self):
        self.api.request.side_effect = httpx.ConnectError("OpenCode unavailable")
        with self.assertRaises(httpx.ConnectError):
            await self.manager.verify("task", "check", self.checks)
        with self.assertRaises(httpx.ConnectError):
            await self.manager.followup("task", "fix", "Review retained changes", "Cause addressed")
        self.assertNotIn("verification", self.manager.get("task"))
        self.assertEqual(self.manager.get("task")["status"], "failed")
        self.api.prompt.assert_not_awaited()
