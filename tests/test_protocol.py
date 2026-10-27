import copy
import json
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
from starlette.testclient import TestClient

from spark_workers import workspace
from spark_workers.manager import Manager
from spark_workers.opencode import OpenCode
from spark_workers.server import build_app

CONFIG = json.loads((Path(__file__).resolve().parent.parent / "config.example.json").read_text())


class ProtocolTests(unittest.TestCase):
    def test_completed_result_verification_and_cleanup_over_mcp(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repo = root / "repo"
            repo.mkdir()
            workspace.git(repo, "init", "-q")
            workspace.git(repo, "config", "user.name", "Test User")
            workspace.git(repo, "config", "user.email", "test@example.test")
            (repo / "code.py").write_text("value = 1\n")
            workspace.git(repo, "add", ".")
            workspace.git(repo, "commit", "-qm", "Initial")
            model_client = httpx.AsyncClient(
                transport=httpx.MockTransport(
                    lambda request: httpx.Response(200, json={"data": [{"id": "qwen-model"}]})
                )
            )
            seed = Manager(root, copy.deepcopy(CONFIG), model_client)
            task_id = "a" * 32
            artifact = seed.state / "jobs" / task_id
            artifact.mkdir(parents=True)
            base = workspace.git(repo, "rev-parse", "HEAD").decode().strip()
            copied = workspace.prepare(repo, base, True, artifact / "worktree", artifact)
            (Path(copied["worktree"]) / "code.py").write_text("value = 2\n")
            job = {
                "id": task_id,
                "request_id": "fixture",
                "backend": "primary",
                "goal": "Fix the numeric value",
                "repository": str(repo),
                "created_at": 1000,
                "status": "completed",
                "session_id": None,
                "workspace": copied,
                "allowed_paths": ["code.py"],
                "summary": "Fixed value",
            }
            job["artifacts"] = workspace.export_changes(job, artifact)
            seed.save(job)
            seed.db.close()
            with patch("spark_workers.server.httpx.AsyncClient", return_value=model_client):
                with TestClient(
                    build_app(root, copy.deepcopy(CONFIG)), base_url="http://127.0.0.1:18990"
                ) as client:
                    client.headers["Accept"] = "application/json, text/event-stream"
                    initialized = client.post(
                        "/mcp",
                        json={
                            "jsonrpc": "2.0",
                            "id": 1,
                            "method": "initialize",
                            "params": {
                                "protocolVersion": "2025-11-25",
                                "capabilities": {},
                                "clientInfo": {"name": "test", "version": "1"},
                            },
                        },
                    )
                    client.headers["Mcp-Session-Id"] = initialized.headers["Mcp-Session-Id"]
                    client.post("/mcp", json={"jsonrpc": "2.0", "method": "notifications/initialized"})

                    def call(name, arguments):
                        response = client.post(
                            "/mcp",
                            json={
                                "jsonrpc": "2.0",
                                "id": 2,
                                "method": "tools/call",
                                "params": {"name": name, "arguments": arguments},
                            },
                        ).json()["result"]
                        self.assertFalse(response.get("isError"), response)
                        return json.loads(response["content"][0]["text"])

                    waited = call("worker_wait", {"task_ids": [task_id], "timeout_seconds": 0})
                    self.assertTrue(waited["ready"])
                    self.assertNotIn("summary", waited["tasks"][0])
                    listed = call(
                        "workers_list",
                        {
                            "status": "completed",
                            "repository": str(repo),
                            "created_after": 1000,
                            "created_before": 1000,
                            "limit": 1,
                        },
                    )
                    self.assertEqual(listed["total"], 1)
                    self.assertEqual(listed["tasks"][0]["id"], task_id)
                    self.assertIsNone(listed["next_offset"])
                    self.assertNotIn("summary", listed["tasks"][0])
                    for invalid in [{"limit": 0}, {"limit": 101}, {"offset": -1}, {"status": "unknown"}]:
                        response = client.post(
                            "/mcp",
                            json={
                                "jsonrpc": "2.0",
                                "id": 3,
                                "method": "tools/call",
                                "params": {"name": "workers_list", "arguments": invalid},
                            },
                        ).json()["result"]
                        self.assertTrue(response["isError"], response)
                    checked = call(
                        "worker_verify",
                        {
                            "task_id": task_id,
                            "request_id": "check",
                            "checks": [
                                {
                                    "argv": [
                                        sys.executable,
                                        "-B",
                                        "-c",
                                        "from code import value; assert value == 2",
                                    ]
                                }
                            ],
                        },
                    )
                    self.assertEqual(checked["status"], "passed")
                    result = call("worker_result", {"task_id": task_id})
                    self.assertEqual(result["verification"]["checks"][0]["exit_code"], 0)
                    call("worker_feedback", {"task_id": task_id, "verdict": "accepted", "rework_minutes": 0})
                    self.assertTrue(call("worker_cleanup", {"task_id": task_id})["cleaned_up"])
                    self.assertFalse(Path(copied["worktree"]).exists())
                    self.assertEqual((repo / "code.py").read_text(), "value = 1\n")

    def test_failed_followup_requires_and_forwards_recovery_reason_over_mcp(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            task_id = "b" * 32
            model_client = httpx.AsyncClient(
                transport=httpx.MockTransport(
                    lambda request: httpx.Response(200, json={"data": [{"id": "qwen-model"}]})
                )
            )
            seed = Manager(root, copy.deepcopy(CONFIG), model_client)
            folder = seed.state / "jobs" / task_id
            (folder / "worktree").mkdir(parents=True)
            (folder / "changes.patch").touch()
            seed.save(
                {
                    "id": task_id,
                    "request_id": "fixture",
                    "backend": "primary",
                    "goal": "Review retained changes",
                    "status": "failed",
                    "error": "Step interrupted",
                    "session_id": "session",
                    "session_confirmed": True,
                    "dispatch": {"id": "msg_initial", "status": "accepted"},
                    "created_at": time.time(),
                    "workspace": {"worktree": str(folder / "worktree")},
                    "artifacts": {"patch": str(folder / "changes.patch")},
                    "allowed_paths": ["code.py"],
                }
            )
            seed.db.close()
            api = SimpleNamespace(
                info=AsyncMock(return_value={"time": {}}),
                request=AsyncMock(return_value={"data": {}}),
                inbox=AsyncMock(return_value=[]),
                messages=AsyncMock(return_value=[]),
                permissions=AsyncMock(return_value=[]),
                prompt=AsyncMock(),
            )
            with (
                patch("spark_workers.server.httpx.AsyncClient", return_value=model_client),
                patch("spark_workers.manager.OpenCode", return_value=api),
                TestClient(
                    build_app(root, copy.deepcopy(CONFIG)), base_url="http://127.0.0.1:18990"
                ) as client,
            ):
                client.headers["Accept"] = "application/json, text/event-stream"
                initialized = client.post(
                    "/mcp",
                    json={
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "initialize",
                        "params": {
                            "protocolVersion": "2025-11-25",
                            "capabilities": {},
                            "clientInfo": {"name": "test", "version": "1"},
                        },
                    },
                )
                client.headers["Mcp-Session-Id"] = initialized.headers["Mcp-Session-Id"]
                client.post("/mcp", json={"jsonrpc": "2.0", "method": "notifications/initialized"})
                arguments = {
                    "task_id": task_id,
                    "request_id": "fix",
                    "instruction": "Review retained changes",
                }
                for reason in [None, "Use a read-only check instead of the rejected shell command"]:
                    if reason:
                        arguments["recovery_reason"] = reason
                    response = client.post(
                        "/mcp",
                        json={
                            "jsonrpc": "2.0",
                            "id": 2,
                            "method": "tools/call",
                            "params": {"name": "worker_followup", "arguments": arguments},
                        },
                    ).json()["result"]
                    if reason is None:
                        self.assertTrue(response["isError"], response)
                        api.prompt.assert_not_awaited()
                    else:
                        self.assertFalse(response.get("isError"), response)
                        self.assertEqual(json.loads(response["content"][0]["text"])["run"], 2)
                        self.assertIn(reason, api.prompt.call_args.args[1])
                api.prompt.assert_awaited_once()

    def test_mcp_discovery_status_and_offline_start(self):
        def model_server(request):
            if request.url.port == 18000:
                return httpx.Response(200, json={"data": [{"id": "qwen-model"}]})
            return httpx.Response(502)

        with tempfile.TemporaryDirectory() as temporary:
            model_client = httpx.AsyncClient(transport=httpx.MockTransport(model_server))
            with patch("spark_workers.server.httpx.AsyncClient", return_value=model_client):
                with TestClient(
                    build_app(temporary, copy.deepcopy(CONFIG)), base_url="http://127.0.0.1:18990"
                ) as client:
                    client.headers["Accept"] = "application/json, text/event-stream"
                    initialized = client.post(
                        "/mcp",
                        json={
                            "jsonrpc": "2.0",
                            "id": 1,
                            "method": "initialize",
                            "params": {
                                "protocolVersion": "2025-11-25",
                                "capabilities": {},
                                "clientInfo": {"name": "test", "version": "1"},
                            },
                        },
                    )
                    self.assertEqual(initialized.status_code, 200)
                    client.headers["Mcp-Session-Id"] = initialized.headers["Mcp-Session-Id"]
                    client.post("/mcp", json={"jsonrpc": "2.0", "method": "notifications/initialized"})
                    catalog = client.post(
                        "/mcp", json={"jsonrpc": "2.0", "id": 2, "method": "tools/list"}
                    ).json()
                    self.assertIn("delegate", {tool["name"] for tool in catalog["result"]["tools"]})
                    status = client.post(
                        "/mcp",
                        json={
                            "jsonrpc": "2.0",
                            "id": 3,
                            "method": "tools/call",
                            "params": {"name": "workers_status", "arguments": {}},
                        },
                    ).json()
                    available = json.loads(status["result"]["content"][0]["text"])["backends"]
                    self.assertEqual(set(available), {"primary"})
                    self.assertEqual(available["primary"]["capacity"], 3)
                    arguments = {
                        "backend": "secondary",
                        "request_id": "offline",
                        "repository": temporary,
                        "base_commit": "abcdef0",
                        "goal": "Inspect this repository",
                        "relevant_files": ["README.md"],
                        "allowed_paths": [],
                        "acceptance_criteria": ["Report findings"],
                    }
                    result = client.post(
                        "/mcp",
                        json={
                            "jsonrpc": "2.0",
                            "id": 4,
                            "method": "tools/call",
                            "params": {"name": "delegate", "arguments": arguments},
                        },
                    ).json()
                    self.assertTrue(result["result"]["isError"])
                    self.assertIn("not reachable", result["result"]["content"][0]["text"])
                    self.assertFalse((Path(temporary) / ".state/jobs").exists())
                    blocked = client.post(
                        "/mcp",
                        headers={"Origin": "https://unrelated.example"},
                        json={"jsonrpc": "2.0", "id": 5, "method": "tools/list"},
                    )
                    self.assertEqual(blocked.status_code, 403)


class OpenCodeContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_messages_follow_all_pages_in_order(self):
        calls = []

        def handler(request):
            calls.append(request)
            if request.url.params.get("cursor") is None:
                return httpx.Response(200, json={"data": [{"id": "first"}], "cursor": {"next": "page-2"}})
            return httpx.Response(200, json={"data": [{"id": "last"}], "cursor": {"next": None}})

        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary) / "service.json"
            state.write_text(json.dumps({"url": "http://127.0.0.1:49374", "password": "test"}))
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                api = OpenCode(client)
                api.state_path = state
                messages = await api.messages("ses_test")

        self.assertEqual([message["id"] for message in messages], ["first", "last"])
        self.assertEqual(calls[0].url.params.get("order"), "asc")
        self.assertEqual(calls[1].url.params.get("cursor"), "page-2")
        self.assertIsNone(calls[1].url.params.get("order"))

    async def test_messages_reject_a_repeated_cursor(self):
        def handler(request):
            return httpx.Response(200, json={"data": [], "cursor": {"next": "same-page"}})

        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary) / "service.json"
            state.write_text(json.dumps({"url": "http://127.0.0.1:49374", "password": "test"}))
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                api = OpenCode(client)
                api.state_path = state
                with self.assertRaisesRegex(ValueError, "repeated message cursor"):
                    await api.messages("ses_test")

    async def test_v2_session_pins_model_and_credentials_are_reloaded(self):
        calls = []

        def handler(request):
            calls.append(request)
            return httpx.Response(200, json={"data": {"id": "ses_test"}})

        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary) / "service.json"
            state.write_text(json.dumps({"url": "http://127.0.0.1:49374", "password": "first"}))
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                api = OpenCode(client)
                api.state_path = state
                model = {"providerID": "primary", "id": "qwen-model"}
                await api.create(Path(temporary), model, [], "Test", "ses_test")
                body = json.loads(calls[0].content)
                self.assertEqual(body["model"], model)
                self.assertEqual(body["location"]["directory"], temporary)
                self.assertEqual(body["id"], "ses_test")
                self.assertEqual(calls[0].url.path, "/api/session")
                state.write_text(json.dumps({"url": "http://127.0.0.1:49375", "password": "second"}))
                await api.prompt("ses_test", "A bounded task", "msg_test")
                self.assertEqual(calls[1].url.port, 49375)
                self.assertNotEqual(calls[0].headers["Authorization"], calls[1].headers["Authorization"])
                self.assertEqual(calls[1].url.path, "/api/session/ses_test/prompt")
                self.assertEqual(json.loads(calls[1].content), {"id": "msg_test", "text": "A bounded task"})
                await api.inbox("ses_test")
                self.assertEqual(calls[2].url.path, "/api/session/ses_test/inbox")
                await api.cancel_input("ses_test", "msg_test")
                self.assertEqual(calls[3].method, "DELETE")
                self.assertEqual(calls[3].url.path, "/api/session/ses_test/inbox/msg_test")
