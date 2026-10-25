import argparse
import asyncio
import json
import shlex
import shutil
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

from spark_workers.workspace import git

ROOT = Path(__file__).resolve().parent.parent


@contextmanager
def scratch_repository():
    (ROOT / ".state").mkdir(exist_ok=True)
    path = Path(tempfile.mkdtemp(prefix="smoke-", dir=ROOT / ".state"))
    try:
        yield path
    finally:
        linked = path / "repo/.git/worktrees"
        if linked.exists() and any(linked.iterdir()):
            print(f"Retained test repository while worktrees still exist: {path}", flush=True)
        else:
            shutil.rmtree(path)


async def main():
    parser = argparse.ArgumentParser(description="Opt-in live OpenCode worker smoke test")
    parser.add_argument("--backend", required=True)
    parser.add_argument("--url", default="http://127.0.0.1:18990/mcp")
    args = parser.parse_args()
    test_command = shlex.join([sys.executable, "-B", "-m", "unittest", "-v"])
    with scratch_repository() as temporary:
        repo = Path(temporary) / "repo"
        repo.mkdir()
        git(repo, "init", "-q")
        git(repo, "config", "user.name", "Test User")
        git(repo, "config", "user.email", "test@example.test")
        (repo / "calculator.py").write_text("def double(value):\n    return value\n")
        (repo / ".gitignore").write_text("__pycache__/\n")
        (repo / "test_calculator.py").write_text(
            "import unittest\nfrom calculator import double\nclass TestCalculator(unittest.TestCase):\n    def test_double(self):\n        self.assertEqual(double(3), 6)\n"
        )
        git(repo, "add", ".")
        git(repo, "commit", "-qm", "Initial")
        base = git(repo, "rev-parse", "HEAD").decode().strip()
        async with streamablehttp_client(args.url) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                names = [t.name for t in (await session.list_tools()).tools]
                assert "delegate" in names, names
                status = json.loads((await session.call_tool("workers_status", {})).content[0].text)
                assert args.backend in status["backends"], status
                print("Tools:", names, flush=True)
                start = await session.call_tool(
                    "delegate",
                    {
                        "backend": args.backend,
                        "request_id": "smoke-" + Path(temporary).name,
                        "repository": str(repo),
                        "base_commit": base,
                        "include_working_tree": True,
                        "goal": f"Fix double(value) in calculator.py so it returns twice the numeric value. Read the existing test and run it with {test_command}. Do not create any other files.",
                        "relevant_files": ["calculator.py", "test_calculator.py"],
                        "allowed_paths": ["calculator.py"],
                        "acceptance_criteria": [
                            "double(3) is 6",
                            "Existing unittest passes",
                            "Only calculator.py is changed",
                        ],
                    },
                )
                if start.isError:
                    raise RuntimeError(start.content)
                job = json.loads(start.content[0].text)
                task_id = job["id"]
                print("Task:", task_id, flush=True)
                try:
                    for _ in range(80):
                        response = await session.call_tool("worker_result", {"task_id": task_id})
                        job = json.loads(response.content[0].text)
                        print(json.dumps(job, ensure_ascii=False), flush=True)
                        for request in job.get("permission_requests", []):
                            allowed = {
                                test_command,
                                "git status --porcelain",
                                "git diff",
                                "git diff -- calculator.py",
                                "git status --porcelain && git diff",
                            }
                            original = request.get("tool_input") or {}
                            cwd = original.get("cwd", original.get("workdir", job["workspace"]["worktree"]))
                            if (
                                request["action"] != "shell"
                                or original.get("command") not in allowed
                                or Path(cwd).resolve() != Path(job["workspace"]["worktree"]).resolve()
                            ):
                                raise RuntimeError("Unexpected permission request: " + json.dumps(request))
                            print("Approving command once:", original["command"], flush=True)
                            await session.call_tool(
                                "worker_permission",
                                {"task_id": task_id, "request_id": request["id"], "approve": True},
                            )
                        if job["status"] in ["completed", "failed", "cancelled"]:
                            break
                        await asyncio.sleep(3)
                    assert job["status"] == "completed", job
                    worktree = Path(job["workspace"]["worktree"])
                    process = await asyncio.create_subprocess_exec(
                        sys.executable, "-B", "-m", "unittest", "-v", cwd=worktree
                    )
                    assert await process.wait() == 0
                    assert (repo / "calculator.py").read_text() == "def double(value):\n    return value\n"
                    git(repo, "apply", "--check", job["artifacts"]["patch"])
                    assert job["artifacts"]["changed_files"] == ["calculator.py"]
                    await session.call_tool(
                        "worker_feedback",
                        {
                            "task_id": task_id,
                            "verdict": "accepted",
                            "rework_minutes": 0,
                            "notes": "Synthetic end-to-end test: patch applies, tests pass, source unchanged",
                        },
                    )
                    cleanup = await session.call_tool("worker_cleanup", {"task_id": task_id})
                    assert not cleanup.isError, cleanup
                    print("SMOKE PASSED", flush=True)
                finally:
                    response = await session.call_tool("worker_result", {"task_id": task_id})
                    job = json.loads(response.content[0].text)
                    if job["status"] not in ["completed", "failed", "cancelled"]:
                        await session.call_tool("worker_cancel", {"task_id": task_id})
                        response = await session.call_tool("worker_result", {"task_id": task_id})
                        final = json.loads(response.content[0].text)
                        job = final
                    if job["status"] in ["completed", "failed", "cancelled"] and not job.get("cleaned_up"):
                        await session.call_tool(
                            "worker_feedback",
                            {
                                "task_id": task_id,
                                "verdict": "rejected",
                                "notes": "Synthetic test aborted",
                            },
                        )
                        await session.call_tool("worker_cleanup", {"task_id": task_id})


if __name__ == "__main__":
    asyncio.run(main())
