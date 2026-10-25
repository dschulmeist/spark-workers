# Spark workers

A shared MCP (Model Context Protocol) scheduler for OpenCode workers. Codex and Claude Code delegate bounded tasks; each task gets an isolated Git worktree, pinned model, durable status and a reviewable patch. OpenCode operates on the host; model inference can run elsewhere.

## Setup

Requires Python 3.12+, Git with a configured author, uv, and a running OpenCode 2 service. The adapter is tested against OpenCode 2.1.4, not its version-1 API.

```sh
uv sync --frozen
cp -n config.example.json config.json
```

Set each backend's `provider` and `model` to its exact OpenCode identifiers, and `base_url` to its model endpoint. The model ID must also appear in that endpoint's `/models` inventory. Start one shared service:

```sh
opencode service start
uv run spark-workers --config config.json
```

The example grants primary=2 and secondary=3 concurrent tasks; primary gets 3 when secondary is unavailable. Backend names are arbitrary. A busy backend is not offline. Returning secondary does not interrupt primary's third task. Limits cover this scheduler, not other users of the model servers.

## Clients

```sh
codex mcp add spark-workers --url http://127.0.0.1:18990/mcp
claude mcp add --scope user --transport http spark-workers http://127.0.0.1:18990/mcp
mkdir -p "$HOME/.codex/skills" "$HOME/.claude/skills"
ln -s "$PWD/skill" "$HOME/.codex/skills/spark-workers"
ln -s "$PWD/skill" "$HOME/.claude/skills/spark-workers"
```

Reconnect clients after installation. The skill requests active delegation of suitable independent work. `workers_status` lists reachable backends; `delegate` remains registered so recovery does not depend on live tool-list updates. Every start rechecks the model inventory and reserves a shared slot. No model fallback is performed.

## Task lifecycle

Start with an explicit base commit, goal, relevant files, allowed edit paths and acceptance criteria. Reuse the same request ID when retrying a start or follow-up. `worker_wait` waits for any/all selected tasks within a whole-call deadline and returns early for approvals. `worker_result` returns details; its optional `run` selects an archived result. Previous summaries, patches and evidence survive follow-ups. Patches are cumulative from the task input, and usage is session-cumulative, not a per-run delta.

`workers_list` finds stored tasks by status, exact repository path, and inclusive creation-time bounds (`created_after` / `created_before`, Unix seconds). Results are newest first, default to 20 (maximum 100), and include `next_offset` for pagination. `worker_result` includes a bounded `opencode_error` with the original error type and message; full response bodies stay in local evidence.

`worker_cancel` retains its slot until execution is confirmed stopped and queued input is removed. Uncertain prompt admission also retains the slot, including across restarts, until the recorded message ID is found in OpenCode. Shell permissions are granted once by the coordinator; recursive delegation is denied.

With `include_working_tree=true`, the base must equal current HEAD. A temporary Git index snapshots tracked and non-ignored untracked files without changing the caller's index. Ignored dependencies/data are not copied; submodules are rejected. Worktrees isolate files, not networks or services. Approved shell commands are not sandboxed.

Inspect patches and integrate them serially; completion is not independent test verification or acceptance. Record review/rework with `worker_feedback`, then call `worker_cleanup` after preserving accepted changes or rejecting the result. Cleanup refuses edits made after export. Tasks survive scheduler restarts without resending prompts. Full evidence stays in `.state/jobs/<task-id>/`; logs rotate in `.state/service.log`.

`worker_verify` independently runs up to three coordinator-approved command argument lists without a shell or model. Commands share a 30-second execution budget; timeouts terminate their process group. Output previews and retained logs are capped. Checks are serialized with lifecycle changes; run longer tests through the main agent. Failed checks block acceptance. If the service is killed during a check, its running marker blocks continuation/cleanup until an operator reconciles the retained logs and process. Commands are trusted, not sandboxed.

Retained patches from stopped `failed` tasks can also be verified; passing checks do not rewrite the failed outcome. To correct a fixable failure, call `worker_followup` with an explicit `recovery_reason`. The previous failure and patch are archived. Original edit permissions, model and total budgets remain in force; uncertain execution or exhausted budgets require reconciliation or a new task. Recovery does not override a denied command.

`workers_metrics` reports runtime, worker usage and review outcomes. Coordinator token counts require explicit measured input. Missing usage remains unknown, and no token-saving percentage is inferred.

The service binds to loopback with browser-origin checks but no separate authentication. Only expose it to trusted local processes. Configuration and `.state/` are excluded from Git and package builds.

## Checks

```sh
uv run ruff check .
uv run python -m unittest discover -s tests -v
uv build
uv run python -u tests/smoke.py --backend primary
```

Unit/protocol tests use no model credentials. The opt-in smoke test performs a real edit and test run on the selected backend. No distribution license has been selected.
