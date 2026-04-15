"""Pipeline utilities: queue iteration and progress helpers."""

import asyncio
from typing import AsyncIterator, Optional, TypeVar

from rich.progress import Progress, TaskID

T = TypeVar("T")


async def iter_queue(q: asyncio.Queue[Optional[T]]) -> AsyncIterator[T]:
    """Yield items from a queue until a None sentinel arrives."""
    while (item := await q.get()) is not None:
        yield item


def grow_total(progress: Progress, task: TaskID, delta: int) -> None:
    """Increment a task's total by delta (handles None -> delta)."""
    progress.update(task, total=(progress.tasks[task].total or 0) + delta)
