import tempfile
import time
import unittest
from pathlib import Path

import httpx

from spark_workers.manager import Manager

CONFIG = {
    "health_failure_threshold": 1,
    "task_timeout_seconds": 1200,
    "max_assistant_turns": 24,
    "backends": {"primary": {"base_url": "http://test/v1", "provider": "p", "model": "m", "capacity": 1}},
}


class RunTests(unittest.IsolatedAsyncioTestCase):
    async def test_followup_preserves_previous_patch_evidence_and_summary(self):
        with tempfile.TemporaryDirectory() as root:
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(lambda r: httpx.Response(200, json={"data": [{"id": "m"}]}))
            ) as client:
                manager = Manager(root, CONFIG, client)
                folder = manager.state / "jobs/abc"
                folder.mkdir(parents=True)
                patch = folder / "changes.patch"
                patch.write_text("first patch")
                (folder / "opencode.json").write_text("first evidence")
                manager.save(
                    {
                        "id": "abc",
                        "request_id": "start",
                        "run": 1,
                        "backend": "primary",
                        "status": "completed",
                        "session_id": "ses_test",
                        "summary": "first result",
                        "artifacts": {"patch": str(patch)},
                        "tokens": {"input": 100},
                    }
                )

                async def refresh(task_id):
                    return manager.get(task_id)

                manager.refresh = refresh

                class FakeOpenCode:
                    async def prompt(self, session, text, message_id):
                        pass

                manager.opencode = FakeOpenCode()
                await manager.followup("abc", "next", "Inspect another detail")
                self.assertEqual(manager.get("abc")["run"], 2)
                self.assertNotIn("summary", manager.get("abc"))
                self.assertNotIn("artifacts", manager.get("abc"))
                patch.write_text("second patch")
                (folder / "opencode.json").write_text("second evidence")
                previous = await manager.result("abc", 1)
                self.assertEqual(previous["summary"], "first result")
                self.assertEqual(Path(previous["artifacts"]["patch"]).read_text(), "first patch")
                self.assertEqual(Path(previous["evidence"]).read_text(), "first evidence")
                self.assertEqual(previous["tokens"]["input"], 100)
                self.assertEqual(previous["usage_scope"], "session_cumulative")
                self.assertTrue(previous["historical"])
                manager.db.close()

    async def test_old_assistant_response_cannot_complete_a_new_run(self):
        with tempfile.TemporaryDirectory() as root:
            async with httpx.AsyncClient() as client:
                manager = Manager(root, CONFIG, client)
                (manager.state / "jobs/abc").mkdir(parents=True)
                manager.save(
                    {
                        "id": "abc",
                        "request_id": "start",
                        "run": 2,
                        "backend": "primary",
                        "status": "running",
                        "session_id": "ses_test",
                        "created_at": time.time(),
                        "submitted_at": time.time() - 1,
                    }
                )

                class FakeOpenCode:
                    async def info(self, session):
                        return {"time": {"idle": time.time() * 1000}, "outcome": "succeeded"}

                    async def messages(self, session):
                        return [
                            {
                                "type": "assistant",
                                "model": {"providerID": "p", "id": "m"},
                                "time": {"created": (time.time() - 10) * 1000},
                                "content": [{"type": "text", "text": "Old run passed"}],
                                "finish": "stop",
                            }
                        ]

                    async def request(self, method, path):
                        return {"data": {}}

                manager.opencode = FakeOpenCode()
                result = await manager.result("abc")
                self.assertEqual(result["status"], "failed")
                self.assertEqual(result["summary"], "")
                manager.db.close()
