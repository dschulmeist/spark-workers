import argparse
import asyncio
import json
import logging
from contextlib import asynccontextmanager
from logging.handlers import RotatingFileHandler
from pathlib import Path

import httpx
import uvicorn
from mcp.server.lowlevel import Server
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import TextContent, Tool, ToolAnnotations
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

from .manager import ACTIVE, TERMINAL, Assignment, Manager
from .verification import Check

LOG = logging.getLogger(__name__)


def load_config(path):
    config = json.loads(Path(path).read_text())
    if not config.get("backends"):
        raise ValueError("Configure at least one backend")
    for name, backend in config["backends"].items():
        if backend["capacity"] < 1 or backend.get("offline_capacity", 1) < 1:
            raise ValueError(f"Backend {name}: capacities must be positive")
        dependency = backend.get("expand_when_unavailable")
        if dependency is not None and (dependency not in config["backends"] or dependency == name):
            raise ValueError(f"Backend {name}: expansion dependency must name another backend")
        if dependency is not None and "offline_capacity" not in backend:
            raise ValueError(f"Backend {name}: expansion requires offline_capacity")
    return config


def schema(properties=None, required=None):
    return {
        "type": "object",
        "properties": properties or {},
        "required": required or [],
        "additionalProperties": False,
    }


TASK_ID = {"type": "string", "pattern": "^[a-f0-9]{32}$"}


def tools():
    read_only = ToolAnnotations(readOnlyHint=True, destructiveHint=False)
    result = [
        Tool(
            name="delegate",
            description="Start a bounded OpenCode task in an isolated Git worktree. Choose backend from workers_status; unavailable or occupied backends reject starts. Returns a task ID immediately.",
            inputSchema={
                **Assignment.model_json_schema(),
                "properties": {**Assignment.model_json_schema()["properties"], "backend": {"type": "string"}},
                "required": [*Assignment.model_json_schema()["required"], "backend"],
            },
        ),
        Tool(
            name="workers_status",
            description="List reachable Spark workers, free slots and recent tasks. Check before delegating independent work.",
            inputSchema=schema(),
            annotations=read_only,
        ),
        Tool(
            name="workers_list",
            description="Find stored tasks, newest first. Filter by status, exact repository path, or inclusive creation-time bounds in Unix seconds. Returns compact results with total count and next_offset; use worker_result for details.",
            inputSchema=schema(
                {
                    "status": {"type": "string", "enum": sorted(ACTIVE | TERMINAL)},
                    "repository": {"type": "string", "minLength": 1},
                    "created_after": {"type": "number", "minimum": 0},
                    "created_before": {"type": "number", "minimum": 0},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 20},
                    "offset": {"type": "integer", "minimum": 0, "default": 0},
                }
            ),
            annotations=read_only,
        ),
        Tool(
            name="worker_result",
            description="Get progress, per-run budget usage, evidence path, final summary and patch paths, or a recorded historical run. Token usage is session-cumulative. Full evidence stays local.",
            inputSchema=schema({"task_id": TASK_ID, "run": {"type": "integer", "minimum": 1}}, ["task_id"]),
            annotations=read_only,
        ),
        Tool(
            name="worker_wait",
            description="Wait up to 30 seconds for any/all selected tasks. Returns the latest checkpoint, activity and budget usage, and returns early for command approvals.",
            inputSchema=schema(
                {
                    "task_ids": {
                        "type": "array",
                        "items": TASK_ID,
                        "minItems": 1,
                        "maxItems": 5,
                        "uniqueItems": True,
                    },
                    "mode": {"type": "string", "enum": ["any", "all"]},
                    "timeout_seconds": {"type": "number", "minimum": 0, "maximum": 30},
                },
                ["task_ids"],
            ),
            annotations=read_only,
        ),
        Tool(
            name="worker_cancel",
            description="Interrupt one worker. Retains partial changes and logs; releases capacity only after execution stops.",
            inputSchema=schema({"task_id": TASK_ID}, ["task_id"]),
        ),
        Tool(
            name="worker_permission",
            description="Approve or reject ONE pending worker command after inspecting its exact action/resources. Never bypass user approval for external writes.",
            inputSchema=schema(
                {"task_id": TASK_ID, "request_id": {"type": "string"}, "approve": {"type": "boolean"}},
                ["task_id", "request_id", "approve"],
            ),
        ),
        Tool(
            name="worker_followup",
            description="Resume a completed worker, or a stopped failed worker with an explicit recovery_reason explaining the addressed cause. Original worktree, edit scope and model remain; each explicit follow-up gets fresh configured run budgets. Does not override denied permissions.",
            inputSchema=schema(
                {
                    "task_id": TASK_ID,
                    "request_id": {"type": "string", "minLength": 1, "maxLength": 100},
                    "instruction": {"type": "string", "minLength": 5, "maxLength": 12000},
                    "recovery_reason": {"type": "string", "minLength": 5, "maxLength": 2000},
                },
                ["task_id", "request_id", "instruction"],
            ),
        ),
        Tool(
            name="worker_verify",
            description="Independently check retained patches from completed or stopped failed tasks, without a model or shell. Passing checks do not change a failed run's status. Maximum three checks and 30 seconds total. Commands are trusted, not sandboxed.",
            inputSchema=schema(
                {
                    "task_id": TASK_ID,
                    "request_id": {"type": "string", "minLength": 1, "maxLength": 100},
                    "checks": {
                        "type": "array",
                        "items": Check.model_json_schema(),
                        "minItems": 1,
                        "maxItems": 3,
                    },
                },
                ["task_id", "request_id", "checks"],
            ),
        ),
        Tool(
            name="worker_feedback",
            description="Record integration verdict, rework and optional measured coordinator tokens. Never invent token savings; unknown usage stays null.",
            inputSchema=schema(
                {
                    "task_id": TASK_ID,
                    "verdict": {"type": "string", "enum": ["accepted", "needs_rework", "rejected"]},
                    "rework_minutes": {"type": ["number", "null"], "minimum": 0},
                    "cloud_input_tokens": {"type": ["integer", "null"], "minimum": 0},
                    "cloud_output_tokens": {"type": ["integer", "null"], "minimum": 0},
                    "notes": {"type": "string", "maxLength": 2000},
                },
                ["task_id", "verdict"],
            ),
        ),
        Tool(
            name="worker_cleanup",
            description="Remove this task's isolated worktree after review/integration or explicit rejection. Requires recorded feedback and retained patch; never deletes the source checkout.",
            inputSchema=schema({"task_id": TASK_ID}, ["task_id"]),
        ),
        Tool(
            name="workers_metrics",
            description="Report local worker tokens, runtime, outcomes, review feedback and coordinator usage when explicitly recorded. No estimated savings.",
            inputSchema=schema(),
            annotations=read_only,
        ),
    ]
    return result


def build_app(root, config):
    server = Server(
        "spark-workers",
        version="0.1.0",
        instructions=(
            "Actively delegate suitable independent work to reachable Spark workers before doing that work yourself. "
            "Check workers_status, then delegate with a reachable backend and bounded assignment. "
            "Continue non-overlapping work while they run. Read results, inspect patches, integrate serially, "
            "record feedback and clean up. No automatic cloud fallback. Existing user approval rules remain in force."
        ),
    )
    manager = None
    http_manager = StreamableHTTPSessionManager(
        server,
        json_response=True,
        security_settings=TransportSecuritySettings(
            enable_dns_rebinding_protection=True,
            allowed_hosts=[f"127.0.0.1:{config['port']}", f"localhost:{config['port']}"],
            allowed_origins=[f"http://127.0.0.1:{config['port']}", f"http://localhost:{config['port']}"],
        ),
    )

    @server.list_tools()
    async def list_tools():
        return tools()

    @server.call_tool()
    async def call_tool(name, arguments):
        if name == "delegate":
            assignment = {key: value for key, value in arguments.items() if key != "backend"}
            result = await manager.start(arguments["backend"], assignment)
        elif name == "workers_status":
            result = {
                "backends": {key: value for key, value in manager.status().items() if value["available"]},
                "recent_tasks": [
                    {"id": j["id"], "backend": j["backend"], "status": j["status"], "goal": j["goal"][:180]}
                    for j in manager.jobs()[:10]
                ],
            }
        elif name == "workers_list":
            result = manager.list_tasks(**arguments)
        elif name == "worker_result":
            result = await manager.result(arguments["task_id"], arguments.get("run"))
        elif name == "worker_wait":
            result = await manager.wait(
                arguments["task_ids"], arguments.get("mode", "all"), arguments.get("timeout_seconds", 20)
            )
        elif name == "worker_cancel":
            result = await manager.cancel(arguments["task_id"])
        elif name == "worker_permission":
            result = await manager.reply(arguments["task_id"], arguments["request_id"], arguments["approve"])
        elif name == "worker_followup":
            result = await manager.followup(
                arguments["task_id"],
                arguments["request_id"],
                arguments["instruction"],
                arguments.get("recovery_reason"),
            )
        elif name == "worker_feedback":
            result = manager.feedback(
                arguments["task_id"],
                arguments["verdict"],
                arguments.get("rework_minutes"),
                arguments.get("cloud_input_tokens"),
                arguments.get("cloud_output_tokens"),
                arguments.get("notes", ""),
            )
        elif name == "worker_verify":
            result = await manager.verify(arguments["task_id"], arguments["request_id"], arguments["checks"])
        elif name == "worker_cleanup":
            result = await manager.cleanup(arguments["task_id"])
        elif name == "workers_metrics":
            result = manager.metrics()
        else:
            raise ValueError("Unknown tool")
        return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False))]

    async def monitor():
        while True:
            try:
                await manager.probe()
                for job in manager.jobs():
                    if job["session_id"]:
                        await manager.refresh(job["id"])
            except Exception:
                LOG.exception("Worker monitor failed")
            await asyncio.sleep(config["poll_seconds"])

    @asynccontextmanager
    async def lifespan(app):
        nonlocal manager
        async with httpx.AsyncClient(trust_env=False) as client:
            manager = Manager(root, config, client)
            await manager.probe()
            await manager.recover()
            monitor_task = asyncio.create_task(monitor())
            async with http_manager.run():
                try:
                    yield
                finally:
                    monitor_task.cancel()
                    await asyncio.gather(monitor_task, return_exceptions=True)
                    if manager.background:
                        await asyncio.gather(*manager.background, return_exceptions=True)
                    manager.db.close()

    class MCPTransport:
        async def __call__(self, scope, receive, send):
            await http_manager.handle_request(scope, receive, send)

    async def health(request):
        return JSONResponse({"service": "spark-workers", "backends": manager.status()})

    return Starlette(
        routes=[
            Route("/health", health),
            Route("/mcp", endpoint=MCPTransport(), methods=["GET", "POST", "DELETE"]),
        ],
        lifespan=lifespan,
    )


def main():
    parser = argparse.ArgumentParser(description="Shared MCP scheduler for OpenCode 2 workers")
    parser.add_argument("--config", type=Path, default=Path("config.json"))
    args = parser.parse_args()
    path = args.config.resolve()
    try:
        config = load_config(path)
    except (OSError, ValueError, KeyError) as error:
        parser.error(f"Cannot load {path}: {error}. Start from config.example.json.")
    root = path.parent
    state = root / ".state"
    state.mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        handlers=[
            RotatingFileHandler(state / "service.log", maxBytes=5_000_000, backupCount=2),
            logging.StreamHandler(),
        ],
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    uvicorn.run(
        build_app(root, config),
        host="127.0.0.1",
        port=config["port"],
        log_level="info",
        log_config=None,
        access_log=False,
    )


if __name__ == "__main__":
    main()
