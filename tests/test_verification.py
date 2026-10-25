import asyncio
import sys
import tempfile
import time
import unittest
from pathlib import Path

import httpx

from spark_workers import workspace
from spark_workers.manager import Manager
from spark_workers.verification import Check, run_check


class CommandTests(unittest.IsolatedAsyncioTestCase):
    async def test_output_is_bounded_and_shell_metacharacters_are_literal(self):
        with tempfile.TemporaryDirectory() as root:
            command = Check(
                argv=[
                    sys.executable,
                    "-c",
                    "import sys; print(sys.argv[1]); print('x'*300000)",
                    "literal;not-a-shell",
                ]
            )
            result = await run_check(command, root, Path(root) / "check.log", 5)
            self.assertEqual(result["exit_code"], 0)
            self.assertIn("literal;not-a-shell", result["output"])
            self.assertLessEqual(len(result["output"]), 2048)
            self.assertLessEqual(Path(result["log"]).stat().st_size, 262144)
            self.assertTrue(result["log_truncated"])

    async def test_timeout_terminates_descendants(self):
        with tempfile.TemporaryDirectory() as root:
            marker = Path(root) / "escaped"
            child = "import time,pathlib; time.sleep(0.6); pathlib.Path('escaped').touch()"
            command = Check(
                argv=[
                    sys.executable,
                    "-c",
                    f"import subprocess,sys,time; subprocess.Popen([sys.executable,'-c',{child!r}]); time.sleep(60)",
                ]
            )
            before = time.monotonic()
            result = await run_check(command, root, Path(root) / "check.log", 0.15)
            self.assertTrue(result["timed_out"])
            self.assertLess(time.monotonic() - before, 1)
            await asyncio.sleep(0.7)
            self.assertFalse(marker.exists())

    async def test_cancelling_check_stops_its_process(self):
        with tempfile.TemporaryDirectory() as root:
            command = Check(
                argv=[
                    sys.executable,
                    "-c",
                    "import time,pathlib; time.sleep(0.6); pathlib.Path('escaped').touch()",
                ]
            )
            task = asyncio.create_task(run_check(command, root, Path(root) / "check.log", 10))
            await asyncio.sleep(0.1)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            await asyncio.sleep(0.7)
            self.assertFalse((Path(root) / "escaped").exists())


class VerificationTests(unittest.IsolatedAsyncioTestCase):
    async def test_uncertain_verification_blocks_lifecycle_after_restart(self):
        with tempfile.TemporaryDirectory() as root:
            async with httpx.AsyncClient() as client:
                original = Manager(root, {"backends": {}}, client)
                original.save(
                    {
                        "id": "abc",
                        "request_id": "task",
                        "status": "completed",
                        "session_id": None,
                        "verification": {
                            "status": "running",
                            "request_id": "interrupted",
                            "specifications": [],
                        },
                    }
                )
                original.db.close()
                recovered = Manager(root, {"backends": {}}, client)
                with self.assertRaisesRegex(ValueError, "uncertain"):
                    await recovered.verify("abc", "new-check", [{"argv": [sys.executable, "-c", "pass"]}])
                with self.assertRaisesRegex(ValueError, "uncertain"):
                    await recovered.followup("abc", "next", "Check another detail")
                with self.assertRaisesRegex(ValueError, "uncertain"):
                    await recovered.cleanup("abc")
                recovered.db.close()

    async def test_false_model_claim_is_rejected_and_check_retry_is_cached(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            repo = root / "repo"
            repo.mkdir()
            workspace.git(repo, "init", "-q")
            workspace.git(repo, "config", "user.name", "Test User")
            workspace.git(repo, "config", "user.email", "test@example.test")
            (repo / "code.py").write_text("value = 1\n")
            workspace.git(repo, "add", ".")
            workspace.git(repo, "commit", "-qm", "Initial")
            async with httpx.AsyncClient() as client:
                manager = Manager(root, {"backends": {}}, client)
                artifact = manager.state / "jobs" / ("a" * 32)
                artifact.mkdir(parents=True)
                baseline = workspace.git(repo, "rev-parse", "HEAD").decode().strip()
                copied = await asyncio.to_thread(
                    workspace.prepare, repo, baseline, True, artifact / "worktree", artifact
                )
                job = {
                    "id": "a" * 32,
                    "request_id": "task",
                    "backend": "test",
                    "status": "completed",
                    "session_id": None,
                    "workspace": copied,
                    "allowed_paths": ["code.py"],
                    "summary": "All tests passed",
                }
                job["artifacts"] = workspace.export_changes(job, artifact)
                manager.save(job)
                checks = [{"argv": [sys.executable, "-B", "-c", "from code import value; assert value == 2"]}]
                report = await manager.verify(job["id"], "check", checks)
                self.assertEqual(report["status"], "failed")
                self.assertNotEqual(report["checks"][0]["exit_code"], 0)
                self.assertEqual(await manager.verify(job["id"], "check", checks), report)
                with self.assertRaises(ValueError):
                    manager.feedback(job["id"], "accepted", None, None, None, "")
                with self.assertRaises(ValueError):
                    await manager.verify(job["id"], "check", [{"argv": [sys.executable, "-c", "pass"]}])
                passed = await manager.verify(
                    job["id"],
                    "other",
                    [{"argv": [sys.executable, "-B", "-c", "from code import value; assert value == 1"]}],
                )
                self.assertEqual(passed["status"], "passed")
                manager.feedback(job["id"], "accepted", 0, None, None, "Checked")
                await manager.cleanup(job["id"])
                manager.db.close()
