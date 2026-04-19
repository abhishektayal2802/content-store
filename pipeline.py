"""Streaming content pipeline: scrape -> extract -> stage, then import.

Stage (GCS upload) runs concurrently with scrape + extract so bytes are
moving as soon as a page is extracted. Import (chunk + embed + index, one
LRO per corpus) runs as a terminal gather *after* the extract/stage queue
drains -- import is a single batch call, not a per-file operation, so
there is no benefit to interleaving it with extraction.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Optional

from infra.content import ExtractedPage, METADATA_SCHEMA, PageMeta
from infra.llm import GeminiRuntime
from infra.rag import VertexRagClient

from .constants import QUEUE_SIZE
from .extractor import Extractor
from .importer import Importer
from .reporter import ProgressReporter
from .scraper import Scraper
from .stager import Stager


class Pipeline:
    """Orchestrates streaming scrape -> extract -> stage, then terminal import."""

    def __init__(self, runtime: GeminiRuntime, rag: VertexRagClient) -> None:
        """Wire up stage-specific workers sharing the same RAG + Gemini clients."""
        self._rag = rag
        self._scraper = Scraper()
        self._extractor = Extractor(runtime)
        self._stager = Stager(rag)
        self._importer = Importer(rag)

        self._pdf_queue: asyncio.Queue[Optional[Path]] = asyncio.Queue(maxsize=QUEUE_SIZE)
        self._page_queue: asyncio.Queue[Optional[ExtractedPage]] = asyncio.Queue(maxsize=QUEUE_SIZE)

    async def run(self) -> None:
        """Setup corpora, stream scrape+extract+stage, then import as terminal."""
        done = await self._setup()
        reporter = ProgressReporter()
        with reporter.live():
            # Streaming phase: scrape -> extract -> stage-to-GCS run in
            # parallel. TaskGroup surfaces the first exception to cancel siblings.
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self._scraper.run(self._pdf_queue, reporter))
                tg.create_task(
                    self._extractor.run(self._pdf_queue, self._page_queue, done, reporter)
                )
                tg.create_task(self._stager.run(self._page_queue, reporter))
            # Terminal phase: all bytes now in GCS. Fire 3 import LROs,
            # wait for all, then attach metadata in parallel.
            await self._importer.run(self._stager.manifest, reporter)

    async def _setup(self) -> set[str]:
        """Ensure corpora + schemas exist on the RAG client; return resume set."""
        await self._rag.ensure_corpora(METADATA_SCHEMA)
        # Resume set: page_keys of PDFs already imported into `pages`.
        done_names = await self._rag.list_file_display_names("pages")
        return {PageMeta.from_display_name(n)[1].page_key for n in done_names}
