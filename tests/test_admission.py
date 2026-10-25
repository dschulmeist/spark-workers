import asyncio
import tempfile
import time
import unittest
from pathlib import Path

import httpx

from spark_workers import workspace
from spark_workers.manager import Manager

CONFIG = {
    "health_failure_threshold": 1,
    "task_timeout_seconds": 1200,
    "max_assistant_turns": 24,
    "backends": {"test": {"base_url": "http://test/v1", "provider": "p", "model": "m", "capacity": 1}},
}


class DelayedOpenCode:
    def __init__(self):
        self.sent = asyncio.Event()
        self.release = asyncio.Event()
        self.message_id = None
        self.admitted = False
        self.queued = False
        self.executing = False
        self.lose_response = False
        self.prompts = 0

    async def create(self, directory, model, permissions, title, session_id):
        return {"id": session_id}

    async def prompt(self, session, text, message_id=None):
        self.prompts += 1
        self.session = session
        self.message_id = message_id
        self.sent.set()
        await self.release.wait()
        if self.lose_response:
            raise httpx.ReadTimeout("Admission response lost")
        self.admitted = True
        self.executing = not self.queued

    async def info(self, session):
        return {"time": {"idle": time.time() * 1000}, "outcome": "interrupted"}

    async def messages(self, session):
        return [{"id": self.message_id, "type": "user"}] if self.admitted and not self.queued else []

    async def inbox(self, session):
        return [{"id": self.message_id}] if self.admitted and self.queued else []

    async def cancel_input(self, session, message_id):
        if self.message_id == message_id:
            self.queued = False

    async def interrupt(self, session):
        self.executing = False

    async def request(self, method, path):
        return {"data": {self.session: {}} if self.executing else {}}


class AdmissionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        repo = root / "repo"
        repo.mkdir()
        workspace.git(repo, "init", "-q")
        workspace.git(repo, "config", "user.name", "Test User")
        workspace.git(repo, "config", "user.email", "test@example.test")
        (repo / "code.py").write_text("value = 1\n")
        workspace.git(repo, "add", ".")
        workspace.git(repo, "commit", "-qm", "Initial")
        self.assignment = {
            "request_id": "delayed",
            "repository": str(repo),
            "base_commit": workspace.git(repo, "rev-parse", "HEAD").decode().strip(),
            "goal": "Inspect the source file",
            "relevant_files": ["code.py"],
            "allowed_paths": [],
            "acceptance_criteria": ["Report findings"],
        }
        self.client = httpx.AsyncClient(
            transport=httpx.MockTransport(lambda r: httpx.Response(200, json={"data": [{"id": "m"}]}))
        )
        self.manager = Manager(root, CONFIG, self.client)
        self.api = DelayedOpenCode()
        self.manager.opencode = self.api

    async def asyncTearDown(self):
        self.api.release.set()
        await asyncio.gather(*self.manager.background, return_exceptions=True)
        self.manager.db.close()
        await self.client.aclose()
        self.temporary.cleanup()

    async def start_pending(self):
        job = await self.manager.start("test", self.assignment)
        await asyncio.wait_for(self.api.sent.wait(), 5)
        return job["id"]

    async def assert_slot_retained(self, task_id):
        self.assertEqual(self.manager.get(task_id)["status"], "cancelling")
        self.assertEqual(self.manager.status()["test"]["free"], 0)
        with self.assertRaises(ValueError):
            await self.manager.cleanup(task_id)
        with self.assertRaisesRegex(ValueError, "occupied"):
            await self.manager.start("test", {**self.assignment, "request_id": "another"})

    async def test_cancel_waits_for_initial_prompt_admission(self):
        task_id = await self.start_pending()
        await self.manager.cancel(task_id)
        await self.assert_slot_retained(task_id)
        self.api.release.set()
        await asyncio.gather(*self.manager.background)
        self.assertFalse(self.api.executing)
        self.assertEqual(self.manager.get(task_id)["status"], "cancelled")
        self.assertEqual(self.manager.status()["test"]["free"], 1)

    async def test_cancel_during_session_creation_never_sends_prompt(self):
        creating = asyncio.Event()
        created = asyncio.Event()

        async def create(directory, model, permissions, title, session_id):
            self.api.session = session_id
            creating.set()
            await created.wait()
            return {"id": session_id}

        self.api.create = create
        job = await self.manager.start("test", self.assignment)
        try:
            await asyncio.wait_for(creating.wait(), 5)
            await self.manager.cancel(job["id"])
        finally:
            created.set()
            self.api.release.set()
            await asyncio.gather(*self.manager.background)
        self.assertEqual(self.api.prompts, 0)
        self.assertFalse(self.api.executing)
        self.assertEqual(self.manager.get(job["id"])["status"], "cancelled")

    async def test_cancel_waits_for_followup_admission_and_removes_queued_input(self):
        task_id = await self.start_pending()
        self.api.release.set()
        await asyncio.gather(*self.manager.background)
        self.api.executing = False
        await self.manager.finalize(self.manager.get(task_id), "completed")
        initial_id = self.api.message_id
        self.api.sent.clear()
        self.api.release.clear()
        self.api.queued = True
        pending = asyncio.create_task(self.manager.followup(task_id, "next", "Inspect another detail"))
        try:
            await asyncio.wait_for(self.api.sent.wait(), 5)
            await self.manager.cancel(task_id)
            self.assertEqual(self.manager.status()["test"]["free"], 0)
            self.api.release.set()
            await pending
            self.assertNotEqual(initial_id, self.api.message_id)
            self.assertFalse(self.api.queued)
            self.assertFalse(self.api.executing)
            self.assertEqual(self.manager.get(task_id)["status"], "cancelled")
        finally:
            self.api.release.set()
            await pending

    async def test_uncertain_admission_survives_restart_until_positive_evidence(self):
        self.api.lose_response = True
        task_id = await self.start_pending()
        self.api.release.set()
        with self.assertLogs("spark_workers.manager", level="ERROR"):
            await asyncio.gather(*self.manager.background)
        await self.assert_slot_retained(task_id)
        self.manager.db.close()
        self.manager = Manager(self.temporary.name, CONFIG, self.client)
        self.manager.opencode = self.api
        await self.manager.probe()
        await self.manager.recover()
        await self.assert_slot_retained(task_id)
        self.api.admitted = True
        self.api.queued = True
        await self.manager.refresh(task_id)
        self.assertEqual(self.manager.get(task_id)["status"], "failed")
        self.assertFalse(self.api.queued)
        self.assertEqual(self.api.prompts, 1)
        self.assertEqual(self.manager.status()["test"]["free"], 1)

    async def test_restart_during_dispatch_does_not_treat_idle_as_stop_confirmation(self):
        task_id = await self.start_pending()
        recovered = Manager(self.temporary.name, CONFIG, self.client)
        recovered.opencode = self.api
        try:
            await recovered.recover()
            self.assertEqual(recovered.get(task_id)["status"], "cancelling")
            self.assertEqual(recovered.status()["test"]["active"], 1)
            self.assertEqual(self.api.prompts, 1)
        finally:
            recovered.db.close()
