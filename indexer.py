"""Poll File Search upload operations until indexing completes."""

from __future__ import annotations

import asyncio
from typing import Optional

from infra.llm import GeminiFilesClient, GeminiRuntime

from .constants import INDEX_POLL_INTERVAL
from .queues import iter_queue
from .reporter import ProgressReporter
from .types import PendingIndex


class Indexer:
    """Polls upload operations until each document is indexed."""

    def __init__(self, runtime: GeminiRuntime) -> None:
        """Initialize indexer with shared Gemini runtime."""
        self._files = GeminiFilesClient(runtime)

    async def run(
        self,
        op_queue: asyncio.Queue[Optional[PendingIndex]],
        reporter: ProgressReporter,
    ) -> None:
        """Consume pending operations, poll until done, report progress."""
        tasks = []
        async for pending in iter_queue(op_queue):
            reporter.grow("index", 1)
            tasks.append(asyncio.create_task(self._poll_one(pending, reporter)))

        await asyncio.gather(*tasks)

    async def _poll_one(self, pending: PendingIndex, reporter: ProgressReporter) -> None:
        """Poll one operation until complete, advance progress on success."""
        operation = pending.operation
        try:
            while not operation.done:
                await asyncio.sleep(INDEX_POLL_INTERVAL)
                operation = await self._files.refresh_upload_operation(operation)

            if operation.error:
                raise RuntimeError(str(operation.error))

            reporter.advance("index")
        except Exception as e:
            reporter.record_error("index", pending.name, e)
