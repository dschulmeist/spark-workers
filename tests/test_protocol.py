import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

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
                await api.prompt("ses_test", "A bounded task")
                self.assertEqual(calls[1].url.port, 49375)
                self.assertNotEqual(calls[0].headers["Authorization"], calls[1].headers["Authorization"])
                self.assertEqual(calls[1].url.path, "/api/session/ses_test/prompt")
