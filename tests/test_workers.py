import asyncio
import json
import tempfile
import time
import unittest
from pathlib import Path

import httpx

from spark_workers import workspace
from spark_workers.manager import (
    Assignment,
    BackendHealth,
    Manager,
    active_elapsed,
    capacity,
    turn_limit_reached,
)
from spark_workers.server import tools

ROOT = Path(__file__).resolve().parent.parent
CONFIG = json.loads((ROOT / "config.example.json").read_text())
CONFIG["backends"] = {"david": CONFIG["backends"]["primary"], "denis": CONFIG["backends"]["secondary"]}
CONFIG["backends"]["david"]["expand_when_unavailable"] = "denis"


class CapacityTests(unittest.TestCase):
    def test_turn_limit_allows_final_response_at_boundary(self):
        self.assertFalse(turn_limit_reached([{"finish": "tool-calls"}] * 24, 24))
        self.assertFalse(turn_limit_reached([{"finish": "tool-calls"}] * 24 + [{"finish": None}], 24))
        self.assertFalse(turn_limit_reached([{"finish": "tool-calls"}] * 24 + [{"finish": "stop"}], 24))
        self.assertTrue(turn_limit_reached([{"finish": "tool-calls"}] * 25, 24))
        self.assertTrue(turn_limit_reached([{"finish": "tool-calls"}] * 25 + [{"finish": "stop"}], 24))

    def test_active_time_excludes_completed_and_current_permission_wait(self):
        job = {"submitted_at": 100, "permission_wait_seconds": 20, "permission_wait_started_at": 160}
        self.assertEqual(active_elapsed(job, 200), 40)

    def test_capacity_switch_and_unavailable(self):
        health = {k: BackendHealth(v) for k, v in CONFIG["backends"].items()}
        self.assertEqual(capacity("david", health, CONFIG["backends"]), 0)
        health["david"].available = True
        self.assertEqual(capacity("david", health, CONFIG["backends"]), 3)
        health["denis"].available = True
        self.assertEqual(capacity("david", health, CONFIG["backends"]), 2)
        self.assertEqual(capacity("denis", health, CONFIG["backends"]), 3)

    def test_path_scope_uses_directory_boundaries(self):
        self.assertTrue(workspace.within_scope("src/a.py", ["src"]))
        self.assertFalse(workspace.within_scope("src-other/a.py", ["src"]))
        self.assertFalse(workspace.within_scope("a.py", []))

    def test_path_validation(self):
        for path in ["../secret", "/etc/passwd", "src/../../secret", ".git/config"]:
            with self.assertRaises(ValueError):
                workspace.validate_paths([path])

    def test_assignment_requires_explicit_commit(self):
        with self.assertRaises(ValueError):
            Assignment(
                request_id="test",
                repository="/tmp/repo",
                base_commit="main",
                goal="Investigate this module",
                relevant_files=["src"],
                allowed_paths=[],
                acceptance_criteria=["Report findings"],
            )


class HealthTests(unittest.IsolatedAsyncioTestCase):
    async def test_malformed_inventory_marks_backend_unavailable(self):
        health = BackendHealth({"base_url": "http://test/v1", "model": "expected"})
        health.available = True
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda request: httpx.Response(200, json={"data": None}))
        ) as client:
            self.assertFalse(await health.probe(client, 1))
            self.assertFalse(health.available)

    async def test_transient_failure_and_recovery(self):
        codes = iter([200, 502, 502, 200])

        def handler(request):
            code = next(codes)
            return httpx.Response(code, json={"data": [{"id": "model"}]})

        health = BackendHealth({"base_url": "http://test/v1", "model": "model"})
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            await health.probe(client, 2)
            self.assertTrue(health.available)
            self.assertFalse(await health.probe(client, 2))
            self.assertTrue(health.available)
            await health.probe(client, 2)
            self.assertFalse(health.available)
            await health.probe(client, 2)
            self.assertTrue(health.available)

    async def test_missing_model_is_not_available(self):
        health = BackendHealth({"base_url": "http://test/v1", "model": "expected"})
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda r: httpx.Response(200, json={"data": [{"id": "other"}]}))
        ) as client:
            self.assertFalse(await health.probe(client, 1))


class WorkspaceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.repo = self.root / "repo"
        self.repo.mkdir()
        workspace.git(self.repo, "init", "-q")
        workspace.git(self.repo, "config", "user.name", "Test User")
        workspace.git(self.repo, "config", "user.email", "test@example.test")
        (self.repo / "a.py").write_text("value = 1\n")
        workspace.git(self.repo, "add", "a.py")
        workspace.git(self.repo, "commit", "-qm", "Initial")
        self.base = workspace.git(self.repo, "rev-parse", "HEAD").decode().strip()

    def test_worker_permissions_deny_unlisted_actions(self):
        self.assertEqual(
            workspace.permissions(self.root / "worktree", []),
            [
                {"action": "*", "resource": "*", "effect": "deny"},
                {"action": "read", "resource": "*", "effect": "allow"},
                {"action": "glob", "resource": "*", "effect": "allow"},
                {"action": "grep", "resource": "*", "effect": "allow"},
                {"action": "shell", "resource": "*", "effect": "ask"},
            ],
        )

    def tearDown(self):
        self.temporary.cleanup()

    def test_dirty_snapshot_preserves_index_and_excludes_ignored_files(self):
        (self.repo / "a.py").write_text("value = 2\n")
        workspace.git(self.repo, "add", "a.py")
        (self.repo / "a.py").write_text("value = 3\n")
        (self.repo / "untracked.txt").write_text("included")
        (self.repo / ".gitignore").write_text("ignored.txt\n")
        (self.repo / "ignored.txt").write_text("not included")
        before = workspace.source_signature(self.repo)
        result = workspace.prepare(self.repo, self.base, True, self.root / "worktree", self.root)
        copied = Path(result["worktree"])
        self.assertEqual((copied / "a.py").read_text(), "value = 3\n")
        self.assertTrue((copied / "untracked.txt").exists())
        self.assertFalse((copied / "ignored.txt").exists())
        self.assertEqual(before, workspace.source_signature(self.repo))
        self.assertEqual(workspace.git(self.repo, "show", ":a.py"), b"value = 2\n")

    def test_patch_contains_only_worker_changes_including_new_files(self):
        result = workspace.prepare(self.repo, self.base, True, self.root / "worktree", self.root)
        copied = Path(result["worktree"])
        (copied / "a.py").write_text("value = 9\n")
        (copied / "new.txt").write_text("new\n")
        exported = workspace.export_changes({"workspace": result, "allowed_paths": ["a.py"]}, self.root)
        self.assertEqual(set(exported["changed_files"]), {"a.py", "new.txt"})
        self.assertEqual(exported["scope_violations"], ["new.txt"])
        workspace.git(self.repo, "apply", "--check", exported["patch"])
        self.assertEqual((self.repo / "a.py").read_text(), "value = 1\n")

    def test_dirty_snapshot_rejects_non_head_base(self):
        (self.repo / "a.py").write_text("value = 4\n")
        workspace.git(self.repo, "commit", "-am", "Second")
        with self.assertRaises(ValueError):
            workspace.prepare(self.repo, self.base, True, self.root / "worktree", self.root)

    def test_cleanup_preserves_changes_made_after_export(self):
        result = workspace.prepare(self.repo, self.base, True, self.root / "worktree", self.root)
        copied = Path(result["worktree"])

        async def check():
            async with httpx.AsyncClient() as client:
                manager = Manager(self.root, CONFIG, client)
                task_id = "a" * 32
                owned = manager.state / "jobs" / task_id / "worktree"
                owned.parent.mkdir(parents=True)
                workspace.git(self.repo, "worktree", "move", str(copied), str(owned))
                result["worktree"] = str(owned)
                job = {
                    "id": task_id,
                    "request_id": "cleanup",
                    "backend": "david",
                    "status": "completed",
                    "session_id": None,
                    "workspace": result,
                    "allowed_paths": ["a.py"],
                    "feedback": {"verdict": "accepted"},
                }
                job["artifacts"] = workspace.export_changes(job, owned.parent)
                manager.save(job)
                (owned / "a.py").write_text("value = 99\n")
                with self.assertRaisesRegex(ValueError, "changed after export"):
                    await manager.cleanup(task_id)
                self.assertEqual((owned / "a.py").read_text(), "value = 99\n")
                self.assertEqual((self.repo / "a.py").read_text(), "value = 1\n")
                manager.db.close()

        asyncio.run(check())

    def test_cleanup_preserves_worktree_when_source_repository_is_missing(self):
        async def check():
            async with httpx.AsyncClient() as client:
                manager = Manager(self.root, CONFIG, client)
                task_id = "b" * 32
                folder = manager.state / "jobs" / task_id
                folder.mkdir(parents=True)
                copied = workspace.prepare(self.repo, self.base, True, folder / "worktree", folder)
                job = {
                    "id": task_id,
                    "request_id": "missing-source",
                    "backend": "david",
                    "status": "completed",
                    "session_id": None,
                    "workspace": copied,
                    "allowed_paths": ["a.py"],
                    "feedback": {"verdict": "accepted"},
                }
                job["artifacts"] = workspace.export_changes(job, folder)
                manager.save(job)
                valuable = Path(copied["worktree"]) / "a.py"
                valuable.write_text("value = 999\n")
                self.repo.rename(self.root / "moved-repo")
                try:
                    with self.assertRaisesRegex(ValueError, "Source repository is unavailable"):
                        await manager.cleanup(task_id)
                    self.assertEqual(valuable.read_text(), "value = 999\n")
                    self.assertFalse(manager.get(task_id).get("cleaned_up", False))
                    self.assertTrue(Path(job["artifacts"]["patch"]).exists())
                finally:
                    (self.root / "moved-repo").rename(self.repo)
                    manager.db.close()

        asyncio.run(check())


class SchedulerTests(unittest.IsolatedAsyncioTestCase):
    async def test_refresh_failure_does_not_overwrite_concurrent_cancellation(self):
        with tempfile.TemporaryDirectory() as temporary:
            async with httpx.AsyncClient() as client:
                manager = Manager(temporary, CONFIG, client)
                manager.save(
                    {
                        "id": "abc",
                        "request_id": "race",
                        "backend": "david",
                        "status": "running",
                        "session_id": "ses_test",
                    }
                )

                class FakeOpenCode:
                    async def info(self, session):
                        current = manager.get("abc")
                        current["status"] = "cancelling"
                        manager.save(current)
                        raise httpx.ConnectError("connection lost")

                manager.opencode = FakeOpenCode()
                await manager.refresh("abc")
                self.assertEqual(manager.get("abc")["status"], "cancelling")
                manager.db.close()

    async def test_restart_reconciles_persisted_execution_without_new_prompt(self):
        with tempfile.TemporaryDirectory() as temporary:
            async with httpx.AsyncClient() as client:
                original = Manager(temporary, CONFIG, client)
                original.save(
                    {
                        "id": "abc",
                        "request_id": "persisted",
                        "backend": "david",
                        "status": "running",
                        "session_id": "ses_test",
                        "created_at": time.time(),
                        "submitted_at": time.time() - 1,
                    }
                )
                (original.state / "jobs/abc").mkdir(parents=True)
                original.db.close()
                recovered = Manager(temporary, CONFIG, client)

                class FakeOpenCode:
                    async def info(self, session):
                        return {"time": {"idle": time.time() * 1000}, "outcome": "succeeded"}

                    async def messages(self, session):
                        return [
                            {
                                "type": "assistant",
                                "model": {
                                    "providerID": CONFIG["backends"]["david"]["provider"],
                                    "id": CONFIG["backends"]["david"]["model"],
                                },
                                "content": [{"type": "text", "text": "Checked the requested files"}],
                                "time": {"created": time.time() * 1000},
                                "finish": "stop",
                            }
                        ]

                    async def request(self, method, path):
                        return {"data": {}}

                    async def prompt(self, session, text):
                        raise AssertionError("Recovery must not resend the prompt")

                recovered.opencode = FakeOpenCode()
                await recovered.recover()
                self.assertEqual(recovered.get("abc")["status"], "completed")
                self.assertEqual(len(recovered.jobs()), 1)
                recovered.db.close()

    async def test_global_slots_and_idempotent_retry(self):
        with tempfile.TemporaryDirectory() as temporary:
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(
                    lambda r: httpx.Response(
                        200, json={"data": [{"id": CONFIG["backends"]["david"]["model"]}]}
                    )
                )
            ) as client:
                manager = Manager(temporary, CONFIG, client)
                gate = asyncio.Event()

                async def prepare(task_id):
                    await gate.wait()

                manager.prepare = prepare
                manager.health["david"].available = True
                manager.health["denis"].available = True
                assignment = dict(
                    request_id="one",
                    repository="/tmp/repo",
                    base_commit="abcdef0",
                    goal="Investigate this module",
                    relevant_files=["src"],
                    allowed_paths=[],
                    acceptance_criteria=["Report findings"],
                )
                first = await manager.start("david", assignment)
                repeated = await manager.start("david", assignment)
                self.assertEqual(first["id"], repeated["id"])
                await manager.start("david", {**assignment, "request_id": "two"})
                with self.assertRaises(ValueError):
                    await manager.start("david", {**assignment, "request_id": "three"})
                manager.health["denis"].available = False
                await manager.start("david", {**assignment, "request_id": "three"})
                manager.health["denis"].available = True
                self.assertEqual(manager.status()["david"]["free"], 0)
                self.assertEqual(manager.status()["david"]["active"], 3)
                with self.assertRaises(ValueError):
                    await manager.start("david", {**assignment, "goal": "A different assignment"})
                gate.set()
                await asyncio.gather(*manager.background)
                manager.db.close()

    async def test_tool_catalog_does_not_depend_on_backend_availability(self):
        with tempfile.TemporaryDirectory() as temporary:
            async with httpx.AsyncClient() as client:
                manager = Manager(temporary, CONFIG, client)
                manager.health["david"].available = True
                names = {t.name for t in tools()}
                self.assertIn("delegate", names)
                manager.health["denis"].available = True
                self.assertEqual(names, {t.name for t in tools()})
                manager.health["david"].available = False
                self.assertEqual(names, {t.name for t in tools()})
                manager.db.close()

    async def test_recovery_does_not_replay_a_running_prompt(self):
        with tempfile.TemporaryDirectory() as temporary:
            async with httpx.AsyncClient() as client:
                manager = Manager(temporary, CONFIG, client)
                manager.save(
                    {
                        "id": "abc",
                        "request_id": "recover",
                        "backend": "david",
                        "status": "running",
                        "session_id": "ses_test",
                    }
                )
                seen = []

                async def refresh(task_id):
                    seen.append(task_id)

                manager.refresh = refresh
                await manager.recover()
                self.assertEqual(seen, ["abc"])
                self.assertEqual(manager.get("abc")["status"], "running")
                manager.db.close()

    async def test_cancel_keeps_slot_until_stop_confirmed(self):
        with tempfile.TemporaryDirectory() as temporary:
            async with httpx.AsyncClient() as client:
                manager = Manager(temporary, CONFIG, client)
                manager.health["david"].available = True
                manager.save(
                    {
                        "id": "abc",
                        "request_id": "cancel",
                        "backend": "david",
                        "status": "running",
                        "session_id": "ses_test",
                    }
                )

                class OfflineOpenCode:
                    async def interrupt(self, session):
                        raise httpx.ConnectError("temporarily offline")

                manager.opencode = OfflineOpenCode()
                with self.assertRaises(httpx.ConnectError):
                    await manager.cancel("abc")
                self.assertEqual(manager.get("abc")["status"], "cancelling")
                self.assertEqual(manager.status()["david"]["active"], 1)
                manager.db.close()

    async def test_unknown_coordinator_tokens_remain_unknown(self):
        with tempfile.TemporaryDirectory() as temporary:
            async with httpx.AsyncClient() as client:
                manager = Manager(temporary, CONFIG, client)
                manager.save(
                    {
                        "id": "abc",
                        "request_id": "metrics",
                        "backend": "david",
                        "status": "completed",
                        "session_id": None,
                    }
                )
                feedback = manager.feedback("abc", "accepted", None, None, None, "Reviewed")
                self.assertIsNone(feedback["coordinator_input_tokens"])
                self.assertIsNone(manager.metrics()["coordinator_tokens"]["coordinator_input_tokens"])
                self.assertIsNone(manager.metrics()["worker_tokens"]["input"])
                self.assertEqual(manager.metrics()["coordinator_usage_samples"], 0)
                self.assertIn("Not inferred", manager.metrics()["savings"])
                manager.db.close()

    async def test_simultaneous_followups_are_idempotent_and_serialized(self):
        with tempfile.TemporaryDirectory() as temporary:
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(
                    lambda request: httpx.Response(
                        200, json={"data": [{"id": CONFIG["backends"]["david"]["model"]}]}
                    )
                )
            ) as client:
                manager = Manager(temporary, CONFIG, client)
                manager.health["david"].available = True
                folder = manager.state / "jobs/abc"
                (folder / "worktree").mkdir(parents=True)
                (folder / "changes.patch").touch()
                manager.save(
                    {
                        "id": "abc",
                        "request_id": "first",
                        "backend": "david",
                        "status": "completed",
                        "session_id": "ses_test",
                        "workspace": {"worktree": str(folder / "worktree")},
                        "artifacts": {"patch": str(folder / "changes.patch")},
                    }
                )

                async def refresh(task_id):
                    return manager.get(task_id)

                manager.refresh = refresh
                sent = []

                class FakeOpenCode:
                    async def prompt(self, session, text, message_id):
                        sent.append(text)
                        await asyncio.sleep(0.01)

                manager.opencode = FakeOpenCode()
                await asyncio.gather(
                    manager.followup("abc", "retry", "Check the result"),
                    manager.followup("abc", "retry", "Check the result"),
                )
                self.assertEqual(len(sent), 1)
                with self.assertRaises(ValueError):
                    await manager.followup("abc", "different", "Make another change")
                self.assertEqual(len(sent), 1)
                manager.db.close()

    async def test_cancellation_is_retried_after_connection_recovers(self):
        with tempfile.TemporaryDirectory() as temporary:
            async with httpx.AsyncClient() as client:
                manager = Manager(temporary, CONFIG, client)
                manager.save(
                    {
                        "id": "abc",
                        "request_id": "cancel",
                        "backend": "david",
                        "status": "cancelling",
                        "session_id": "ses_test",
                        "created_at": time.time(),
                        "submitted_at": time.time() - 1,
                    }
                )
                (manager.state / "jobs/abc").mkdir(parents=True)
                interrupts = []

                class FakeOpenCode:
                    async def info(self, session):
                        return {"time": {"idle": time.time() * 1000}, "outcome": "interrupted"}

                    async def messages(self, session):
                        return []

                    async def interrupt(self, session):
                        interrupts.append(session)

                    async def request(self, method, path):
                        return {"data": {}}

                manager.opencode = FakeOpenCode()
                result = await manager.refresh("abc")
                self.assertEqual(interrupts, ["ses_test"])
                self.assertEqual(result["status"], "cancelled")
                manager.db.close()

    async def test_empty_or_tool_only_terminal_response_is_not_success(self):
        for content, finish in [
            ([], "stop"),
            ([{"type": "text", "text": "I will inspect this"}], "tool-calls"),
        ]:
            with self.subTest(content=content, finish=finish), tempfile.TemporaryDirectory() as temporary:
                async with httpx.AsyncClient() as client:
                    manager = Manager(temporary, CONFIG, client)
                    manager.save(
                        {
                            "id": "abc",
                            "request_id": "empty",
                            "backend": "david",
                            "status": "running",
                            "session_id": "ses_test",
                            "created_at": time.time(),
                            "submitted_at": time.time() - 1,
                        }
                    )
                    (manager.state / "jobs/abc").mkdir(parents=True)

                    class FakeOpenCode:
                        async def info(self, session):
                            return {"time": {"idle": time.time() * 1000}, "outcome": "succeeded"}

                        async def messages(self, session):
                            return [
                                {
                                    "type": "assistant",
                                    "model": {
                                        "providerID": CONFIG["backends"]["david"]["provider"],
                                        "id": CONFIG["backends"]["david"]["model"],
                                    },
                                    "content": content,
                                    "finish": finish,
                                }
                            ]

                        async def request(self, method, path):
                            return {"data": {}}

                    manager.opencode = FakeOpenCode()
                    result = await manager.refresh("abc")
                    self.assertEqual(result["status"], "failed")
                    self.assertIn("final assistant response", result["error"])
                    manager.db.close()

    async def test_cleanup_rejects_missing_review(self):
        with tempfile.TemporaryDirectory() as temporary:
            async with httpx.AsyncClient() as client:
                manager = Manager(temporary, CONFIG, client)
                manager.save(
                    {
                        "id": "abc",
                        "request_id": "cleanup",
                        "backend": "david",
                        "status": "completed",
                        "session_id": None,
                    }
                )
                with self.assertRaises(ValueError):
                    await manager.cleanup("abc")
                manager.db.close()


if __name__ == "__main__":
    unittest.main()
