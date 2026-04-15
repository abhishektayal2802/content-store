"""Streaming content store pipeline: scrape -> extract -> persist."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Optional

from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn

from infra.llm import GeminiRuntime

from .constants import QUEUE_SIZE
from .extractor import Extractor
from .persister import Persister
from .scraper import Scraper
from .types import ExtractedPage


class Pipeline:
    """Orchestrates streaming scrape -> extract -> persist pipeline."""

    def __init__(self, runtime: GeminiRuntime) -> None:
        """Initialize pipeline with shared runtime and workers."""
        self._runtime = runtime
        self._scraper = Scraper()
        self._extractor = Extractor(runtime)
        self._persister = Persister(runtime)

        self._pdf_queue: asyncio.Queue[Optional[Path]] = asyncio.Queue(maxsize=QUEUE_SIZE)
        self._page_queue: asyncio.Queue[Optional[ExtractedPage]] = asyncio.Queue(maxsize=QUEUE_SIZE)

    async def run(self) -> None:
        """Run the streaming pipeline with concurrent workers."""
        stores, done = await self._persister.setup()

        try:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TextColumn("{task.completed}/{task.total}"),
                TimeElapsedColumn(),
            ) as progress:
                scrape_task = progress.add_task("Scraping", total=None)
                extract_task = progress.add_task("Extracting", total=None)
                persist_task = progress.add_task("Persisting", total=None)

                async with asyncio.TaskGroup() as tg:
                    tg.create_task(
                        self._scraper.run(self._pdf_queue, progress, scrape_task)
                    )
                    tg.create_task(
                        self._extractor.run(
                            self._pdf_queue, self._page_queue, done, progress, extract_task
                        )
                    )
                    tg.create_task(
                        self._persister.run(
                            stores, self._page_queue, progress, persist_task
                        )
                    )
        finally:
            await self._scraper.close()
