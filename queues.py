"""Pipeline utilities: queue iteration."""

import asyncio
from typing import AsyncIterator, Optional, TypeVar

T = TypeVar("T")


async def iter_queue(q: asyncio.Queue[Optional[T]]) -> AsyncIterator[T]:
    """Yield items from a queue until a None sentinel arrives."""
    while (item := await q.get()) is not None:
        yield item
