"""Pipeline: stream scrape -> extract to local cache, then publish to GCS + Vertex.

The local extracted-page cache is the only incremental checkpoint. The publish
phase treats GCS + Vertex corpora as rebuildable projections of that cache.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Optional

from infra.llm import GeminiRuntime
from infra.rag import VertexRagWriter
from infra.storage import GcsBucket

from .cache import PageCache
from .constants import QUEUE_SIZE
from .extractor import Extractor
from .publisher import Publisher
from .reporter import ProgressReporter
from .scraper import Scraper


class Pipeline:
    """Two-phase runner: streaming scrape+extract, then barrier publish from cache."""

    def __init__(
        self, runtime: GeminiRuntime, rag: VertexRagWriter, bucket: GcsBucket,
    ) -> None:
        self._scraper = Scraper()
        self._extractor = Extractor(runtime)
        self._publisher = Publisher(rag, bucket)
        self._cache = PageCache()
        # Only the scrape->extract boundary is streamed; the publish phase is a barrier.
        self._pdf_queue: asyncio.Queue[Optional[Path]] = asyncio.Queue(maxsize=QUEUE_SIZE)

    async def run(self) -> None:
        """Stream scrape+extract in parallel; publish once the cache is closed."""
        reporter = ProgressReporter()
        with reporter.live():
            # Phase 1: scrape -> extract concurrently via pdf_queue.
            # TaskGroup surfaces the first exception to cancel siblings.
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self._scraper.run(self._pdf_queue, reporter.scrape))
                tg.create_task(self._extractor.run(
                    self._pdf_queue, self._cache, reporter.extract,
                ))
            # Phase 2: barrier. The cache is complete; rebuild remote from it.
            await self._publisher.run(self._cache, reporter)
