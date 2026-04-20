"""Streaming pipeline: scrape + extract + stage concurrently, then import LROs."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Optional

from infra.content import ExtractedPage, METADATA_SCHEMA
from infra.llm import GeminiRuntime
from infra.rag import VertexRagWriter

from .constants import QUEUE_SIZE
from .extractor import Extractor
from .importer import Importer
from .reporter import ProgressReporter
from .scraper import Scraper
from .stager import Stager


class Pipeline:
    """Orchestrates streaming scrape -> extract -> stage, then terminal import."""

    def __init__(self, runtime: GeminiRuntime, rag: VertexRagWriter) -> None:
        self._rag = rag
        self._scraper = Scraper()
        self._extractor = Extractor(runtime)
        self._stager = Stager(rag)
        self._importer = Importer(rag)

        self._pdf_queue: asyncio.Queue[Optional[Path]] = asyncio.Queue(maxsize=QUEUE_SIZE)
        self._page_queue: asyncio.Queue[Optional[ExtractedPage]] = asyncio.Queue(maxsize=QUEUE_SIZE)

    async def run(self) -> None:
        """Setup corpora, stream scrape+extract+stage, then import as terminal."""
        staged_page_keys = await self._setup()
        reporter = ProgressReporter()
        with reporter.live():
            # Streaming phase: scrape -> extract -> stage run in parallel.
            # TaskGroup surfaces the first exception to cancel siblings.
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self._scraper.run(self._pdf_queue, reporter.scrape))
                tg.create_task(self._extractor.run(
                    self._pdf_queue, self._page_queue, staged_page_keys, reporter.extract,
                ))
                tg.create_task(self._stager.run(self._page_queue, reporter.stage))
            # Terminal phase: fire 3 import LROs, then attach metadata.
            await self._importer.run(self._stager.manifest, reporter.importer)

    async def _setup(self) -> set[str]:
        """Ensure corpora + schemas exist; return page_keys with GCS sentinels."""
        await self._rag.ensure_corpora(METADATA_SCHEMA)
        return await self._rag.list_sentinels()
