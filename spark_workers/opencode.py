import json
from pathlib import Path

import httpx


class OpenCode:
    def __init__(self, client):
        self.client = client
        self.state_path = Path.home() / ".local/state/opencode/service.json"

    async def request(self, method, path, body=None, params=None):
        state = json.loads(self.state_path.read_text())
        response = await self.client.request(
            method,
            state["url"] + path,
            json=body,
            params=params,
            auth=httpx.BasicAuth("opencode", state["password"]),
            timeout=20,
        )
        response.raise_for_status()
        return response.json() if response.content else None

    async def create(self, directory, model, permissions, title, session_id):
        response = await self.request(
            "POST",
            "/api/session",
            {
                "id": session_id,
                "title": title,
                "agent": "build",
                "model": model,
                "location": {"directory": str(directory)},
                "permissions": permissions,
            },
        )
        return response["data"]

    async def prompt(self, session, text, message_id):
        return await self.request("POST", f"/api/session/{session}/prompt", {"id": message_id, "text": text})

    async def inbox(self, session):
        return (await self.request("GET", f"/api/session/{session}/inbox"))["data"]

    async def cancel_input(self, session, message_id):
        return await self.request("DELETE", f"/api/session/{session}/inbox/{message_id}")

    async def interrupt(self, session):
        return await self.request("POST", f"/api/session/{session}/interrupt", {})

    async def info(self, session):
        return (await self.request("GET", f"/api/session/{session}"))["data"]

    async def messages(self, session):
        messages = []
        params = {"limit": "200", "order": "asc"}
        seen = set()
        while True:
            response = await self.request("GET", f"/api/session/{session}/message", params=params)
            messages.extend(response["data"])
            cursor = (response.get("cursor") or {}).get("next")
            if not cursor:
                return messages
            if cursor in seen:
                raise ValueError("OpenCode returned a repeated message cursor")
            seen.add(cursor)
            params = {"limit": "200", "cursor": cursor}

    async def permissions(self, session):
        return (await self.request("GET", f"/api/session/{session}/permission"))["data"]

    async def permission_reply(self, session, request, approve):
        return await self.request(
            "POST",
            f"/api/session/{session}/permission/{request}/reply",
            {
                "decision": "once" if approve else "reject",
            },
        )
