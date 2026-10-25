import asyncio
import os
import signal
import time
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field


class Check(BaseModel):
    model_config = ConfigDict(extra="forbid")
    argv: list[str] = Field(min_length=1, max_length=32)
    timeout_seconds: float = Field(default=15, gt=0, le=30)


async def run_check(check, directory, log_path, timeout_seconds):
    started = time.monotonic()
    result = {"argv": check.argv, "exit_code": None, "timed_out": False, "log": str(log_path)}
    preview = bytearray()
    total = 0
    try:
        process = await asyncio.create_subprocess_exec(
            *check.argv,
            cwd=directory,
            start_new_session=True,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
    except OSError as error:
        return {**result, "error": str(error), "duration_seconds": round(time.monotonic() - started, 3)}

    async def capture():
        nonlocal total
        with Path(log_path).open("wb") as log:
            while chunk := await process.stdout.read(8192):
                preview.extend(chunk[: max(0, 2048 - len(preview))])
                if total < 262144:
                    log.write(chunk[: 262144 - total])
                total += len(chunk)

    reader = asyncio.create_task(capture())
    try:
        async with asyncio.timeout(timeout_seconds):
            await asyncio.gather(process.wait(), asyncio.shield(reader))
    except TimeoutError:
        result["timed_out"] = True
    finally:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        await process.wait()
        await reader
    return {
        **result,
        "exit_code": process.returncode,
        "duration_seconds": round(time.monotonic() - started, 3),
        "output": preview.decode(errors="replace"),
        "output_truncated": total > len(preview),
        "log_truncated": total > 262144,
    }
