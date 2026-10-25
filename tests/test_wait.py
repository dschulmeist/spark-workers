import asyncio
import tempfile
import time
import unittest

import httpx

from spark_workers.manager import Manager


class WaitTests(unittest.IsolatedAsyncioTestCase):
    async def test_wait_uses_monitored_state_and_stays_within_deadline(self):
        with tempfile.TemporaryDirectory() as root:
            async with httpx.AsyncClient() as client:
                manager = Manager(root, {"backends": {}}, client)
                manager.save(
                    {
                        "id": "one",
                        "request_id": "one",
                        "status": "running",
                        "backend": "test",
                        "session_id": None,
                    }
                )

                async def refresh(task_id):
                    started.set()
                    await asyncio.sleep(10)

                started = asyncio.Event()
                manager.refresh = refresh
                before = time.monotonic()
                result = await manager.wait(["one"], "all", 0.02)
                self.assertTrue(result["timed_out"])
                self.assertFalse(started.is_set())
                self.assertLess(time.monotonic() - before, 0.5)
                manager.db.close()

    async def test_any_all_timeout_and_permission(self):
        with tempfile.TemporaryDirectory() as root:
            async with httpx.AsyncClient() as client:
                manager = Manager(root, {"backends": {}}, client)
                for task_id, status in [("one", "completed"), ("two", "running")]:
                    manager.save(
                        {
                            "id": task_id,
                            "request_id": task_id,
                            "status": status,
                            "backend": "test",
                            "session_id": None,
                        }
                    )

                async def refresh(task_id):
                    return manager.get(task_id)

                manager.refresh = refresh
                self.assertTrue((await manager.wait(["one", "two"], "any", 0))["ready"])
                all_tasks = await manager.wait(["one", "two"], "all", 0)
                self.assertFalse(all_tasks["ready"])
                self.assertTrue(all_tasks["timed_out"])
                second = manager.get("two")
                second["status"] = "waiting_permission"
                manager.save(second)
                approval = await manager.wait(["two"], "all", 30)
                self.assertFalse(approval["timed_out"])
                self.assertEqual(approval["tasks"][0]["status"], "waiting_permission")
                manager.db.close()
