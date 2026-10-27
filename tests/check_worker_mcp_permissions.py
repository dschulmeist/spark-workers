import asyncio
import tempfile
import uuid
from pathlib import Path

import httpx

from spark_workers.opencode import OpenCode
from spark_workers.server import load_config
from spark_workers.workspace import permissions


async def main():
    root = Path(__file__).resolve().parent.parent
    config = load_config(root / "config.json")
    backend = next(iter(config["backends"].values()))
    session_id = "ses_" + uuid.uuid4().hex
    action = "spark-workers_worker_permission"

    with tempfile.TemporaryDirectory(prefix="spark-permission-") as temporary:
        directory = Path(temporary)
        async with httpx.AsyncClient(trust_env=False) as client:
            api = OpenCode(client)
            session_attempted = False
            try:
                session_attempted = True
                session = await api.create(
                    directory,
                    {"providerID": backend["provider"], "id": backend["model"]},
                    permissions(directory, []),
                    "Spark permission integration check",
                    session_id,
                )
                session_id = session.get("id", session_id)

                async def permission_effect():
                    result = await api.request(
                        "POST",
                        f"/api/session/{session_id}/permission",
                        {
                            "action": action,
                            "resources": ["*"],
                            "agent": "build",
                            "source": {
                                "type": "tool",
                                "messageID": "msg_" + uuid.uuid4().hex,
                                "id": "call_" + uuid.uuid4().hex,
                            },
                        },
                    )
                    return result["data"]["effect"]

                denied = await permission_effect()
                if denied != "deny":
                    raise AssertionError(f"Expected deny for {action}, got {denied!r}")

                await api.request(
                    "PATCH",
                    f"/api/session/{session_id}",
                    {
                        "permissions": [
                            *permissions(directory, []),
                            {"action": action, "resource": "*", "effect": "allow"},
                        ]
                    },
                )
                allowed = await permission_effect()
                if allowed != "allow":
                    raise AssertionError(f"Positive control failed for {action}: got {allowed!r}")
                print(f"OpenCode denied {action}; the same action matched an explicit allow rule.")
            finally:
                if session_attempted:
                    try:
                        await api.request("DELETE", f"/api/session/{session_id}")
                    except httpx.HTTPStatusError as error:
                        if error.response.status_code != 404:
                            raise


if __name__ == "__main__":
    asyncio.run(main())
