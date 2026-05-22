"""PDF splitting and LLM extraction; persists results to the local page cache."""

from __future__ import annotations

import asyncio
import base64
from pathlib import Path
from typing import Optional

from infra.content import PageExtraction, PageMeta
from infra.llm import InlineMediaContent, OpenAIResponsesClient, OpenAIRuntime, TextContent
from infra.llm.constants import RESPONSE_CONCURRENCY_LIMIT

from .cache import PageCache
from .constants import (
    EXTRACTION_MODEL,
    EXTRACTION_QUEUE_SIZE,
    EXTRACTION_REASONING_EFFORT,
    EXTRACTION_VERBOSITY,
)
from .pdf import split_pdf
from .prompts import EXTRACTION_PROMPT
from .queues import iter_queue
from .reporter import StageReporter
from .types import CachedPage


class Extractor:
    """Splits input PDFs and extracts content via OpenAI; caches results to disk."""

    def __init__(self, runtime: OpenAIRuntime) -> None:
        self._responses = OpenAIResponsesClient(runtime)

    async def run(
        self,
        pdf_queue: asyncio.Queue[Optional[Path]],
        cache: PageCache,
        reporter: StageReporter,
    ) -> None:
        """Drain the scraper's PDF queue; extract + cache every not-yet-cached page."""
        page_queue: asyncio.Queue[Optional[tuple[PageMeta, bytes]]] = asyncio.Queue(
            maxsize=EXTRACTION_QUEUE_SIZE,
        )
        async with asyncio.TaskGroup() as tg:
            for _ in range(RESPONSE_CONCURRENCY_LIMIT):
                tg.create_task(self._extract_worker(page_queue, cache, reporter))
            async for pdf_path in iter_queue(pdf_queue):
                # Split once per chapter PDF, then filter by cache presence (the resume signal).
                pages = await self._split_missing(pdf_path, cache)
                reporter.grow(len(pages))
                for page in pages:
                    await page_queue.put(page)
            for _ in range(RESPONSE_CONCURRENCY_LIMIT):
                await page_queue.put(None)

    # --- PDF splitting ---

    async def _split_missing(
        self, pdf_path: Path, cache: PageCache,
    ) -> list[tuple[PageMeta, bytes]]:
        """Split one PDF and filter out pages whose extraction is already cached."""
        page_bytes_list = await split_pdf(pdf_path)
        return [
            (meta, page_bytes)
            for i, page_bytes in enumerate(page_bytes_list, 1)
            if not cache.exists(meta := self._meta_from_path(pdf_path, i))
        ]

    def _meta_from_path(self, pdf_path: Path, page: int) -> PageMeta:
        """Reverse the scraper's inputs/ layout into a PageMeta."""
        return PageMeta(
            grade=int(pdf_path.parent.parent.parent.name),
            subject=pdf_path.parent.parent.name,
            book=pdf_path.parent.name,
            chapter=pdf_path.stem,
            page=page,
        )

    # --- LLM extraction ---

    async def _extract_worker(
        self,
        page_queue: asyncio.Queue[Optional[tuple[PageMeta, bytes]]],
        cache: PageCache,
        reporter: StageReporter,
    ) -> None:
        """Drain bounded page work and extract each page with existing cache semantics."""
        async for meta, page_bytes in iter_queue(page_queue):
            await self._extract_one(meta, page_bytes, cache, reporter)

    async def _extract_one(
        self,
        meta: PageMeta,
        pdf_bytes: bytes,
        cache: PageCache,
        reporter: StageReporter,
    ) -> None:
        """Extract one page end-to-end; errors are scoped so siblings keep working."""
        try:
            extraction = await self._extract_page(pdf_bytes, f"{meta.page_key}.pdf")
            # Persist *before* reporting progress -- the cache file is the true checkpoint.
            cache.write(CachedPage(meta=meta, extraction=extraction))
            reporter.advance()
        except Exception as e:
            reporter.record_error(meta.page_key, e)

    async def _extract_page(self, pdf_bytes: bytes, filename: str) -> PageExtraction:
        """Run one stateless structured extraction call for a single page PDF."""
        encoded_pdf = base64.b64encode(pdf_bytes).decode("ascii")

        return await self._responses.chat(
            model=EXTRACTION_MODEL,
            conversation_id=None,
            system_instruction=EXTRACTION_PROMPT,
            input_message=[
                TextContent(text="Extract this single textbook page."),
                InlineMediaContent(
                    data=encoded_pdf,
                    filename=filename,
                ),
            ],
            response_schema=PageExtraction,
            reasoning_effort=EXTRACTION_REASONING_EFFORT,
            verbosity=EXTRACTION_VERBOSITY,
            functions=(),
            hosted=(),
        )
