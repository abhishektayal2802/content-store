"""Streaming content store pipeline: scrape -> extract -> persist -> index."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Optional

from infra.llm import GeminiRuntime

from .constants import QUEUE_SIZE
from .extractor import Extractor
from .indexer import Indexer
from .persister import Persister
from .reporter import ProgressReporter
from .scraper import Scraper
from .types import ExtractedPage, PendingIndex


class Pipeline:
    """Orchestrates streaming scrape -> extract -> persist -> index pipeline."""

    def __init__(self, runtime: GeminiRuntime) -> None:
        """Initialize pipeline with shared runtime and workers."""
        self._runtime = runtime
        self._scraper = Scraper()
        self._extractor = Extractor(runtime)
        self._persister = Persister(runtime)
        self._indexer = Indexer(runtime)

        self._pdf_queue: asyncio.Queue[Optional[Path]] = asyncio.Queue(maxsize=QUEUE_SIZE)
        self._page_queue: asyncio.Queue[Optional[ExtractedPage]] = asyncio.Queue(maxsize=QUEUE_SIZE)
        self._op_queue: asyncio.Queue[Optional[PendingIndex]] = asyncio.Queue(maxsize=QUEUE_SIZE)

    async def run(self) -> None:
        """Run the streaming pipeline with concurrent workers."""
        stores, done = await self._persister.setup()
        reporter = ProgressReporter()

        try:
            with reporter.live():
                async with asyncio.TaskGroup() as tg:
                    tg.create_task(self._scraper.run(self._pdf_queue, reporter))
                    tg.create_task(
                        self._extractor.run(self._pdf_queue, self._page_queue, done, reporter)
                    )
                    tg.create_task(
                        self._persister.run(
                            stores, self._page_queue, self._op_queue, reporter
                        )
                    )
                    tg.create_task(self._indexer.run(self._op_queue, reporter))
        finally:
            await self._scraper.close()
