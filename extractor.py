"""PDF splitting and LLM extraction into the GCS extracted-page cache."""

from __future__ import annotations

import asyncio
import base64
import hashlib
from typing import Optional

from infra.content import PageExtraction, PageMeta
from infra.llm import InlineMediaContent, OpenAIResponsesClient, OpenAIRuntime, TextContent
from infra.llm.constants import RESPONSE_CONCURRENCY_LIMIT
from infra.platform.retry import retry

from .constants import (
    EXTRACTION_MODEL,
    EXTRACTION_QUEUE_SIZE,
    EXTRACTION_REASONING_EFFORT,
    EXTRACTION_VERBOSITY,
)
from .pdf import split_pdf
from .prompts import EXTRACTION_PROMPT
from .run_state import StageRun
from .storage import ContentStoreStorage
from .types import CachedPage, RawChapter


class Extractor:
    """Splits raw PDFs and extracts content via OpenAI; caches JSON in GCS."""

    def __init__(self, runtime: OpenAIRuntime, storage: ContentStoreStorage) -> None:
        self._responses = OpenAIResponsesClient(runtime)
        self._storage = storage

    async def run(
        self,
        stage: StageRun,
        task_index: int,
        task_count: int,
    ) -> None:
        """Extract this Cloud Run task's shard of all raw pages."""
        raw_chapters = await self._storage.list_raw_chapters()
        await stage.start(0)
        page_queue: asyncio.Queue[Optional[tuple[PageMeta, bytes]]] = asyncio.Queue(
            maxsize=EXTRACTION_QUEUE_SIZE,
        )
        async with asyncio.TaskGroup() as tg:
            for _ in range(RESPONSE_CONCURRENCY_LIMIT):
                tg.create_task(self._extract_worker(page_queue, stage))
            for chapter in raw_chapters:
                pages, assigned, skipped = await self._split_missing(chapter, task_index, task_count)
                await stage.planned(assigned)
                await stage.skipped(skipped)
                for page in pages:
                    await page_queue.put(page)
            for _ in range(RESPONSE_CONCURRENCY_LIMIT):
                await page_queue.put(None)

    # --- PDF splitting ---

    async def _split_missing(
        self,
        chapter: RawChapter,
        task_index: int,
        task_count: int,
    ) -> tuple[list[tuple[PageMeta, bytes]], int, int]:
        """Split one raw chapter and return this task's uncached pages."""
        page_bytes_list = await split_pdf(await self._storage.download_raw_chapter(chapter))
        missing: list[tuple[PageMeta, bytes]] = []
        assigned = 0
        skipped = 0
        for i, page_bytes in enumerate(page_bytes_list, 1):
            meta = self._meta_from_chapter(chapter, i)
            if _task_index(meta.page_key, task_count) != task_index:
                continue
            assigned += 1
            if await self._storage.extracted_page_exists(meta):
                skipped += 1
                continue
            missing.append((meta, page_bytes))
        return missing, assigned, skipped

    def _meta_from_chapter(self, chapter: RawChapter, page: int) -> PageMeta:
        """Build page provenance from one raw chapter descriptor."""
        return PageMeta(
            grade=chapter.grade,
            subject=chapter.subject,
            book=chapter.book,
            chapter=chapter.chapter,
            page=page,
        )

    # --- LLM extraction ---

    async def _extract_worker(
        self,
        page_queue: asyncio.Queue[Optional[tuple[PageMeta, bytes]]],
        stage: StageRun,
    ) -> None:
        """Drain bounded page work and extract each real page."""
        while True:
            item = await page_queue.get()
            if item is None:
                return
            meta, page_bytes = item
            await self._extract_one(meta, page_bytes, stage)

    async def _extract_one(
        self,
        meta: PageMeta,
        pdf_bytes: bytes,
        stage: StageRun,
    ) -> None:
        """Extract one page end-to-end and fail loudly after recording context."""
        try:
            extraction = await self._extract_page(pdf_bytes, f"{meta.page_key}.pdf")
            await self._storage.write_extracted_page(CachedPage(meta=meta, extraction=extraction))
            await stage.completed()
        except Exception as e:
            await stage.record_error(meta.page_key, e)
            raise

    @retry()
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


def _task_index(page_key: str, task_count: int) -> int:
    """Stable page-key shard index for Cloud Run Job tasks."""
    digest = hashlib.sha256(page_key.encode("utf-8")).hexdigest()
    return int(digest, 16) % task_count
