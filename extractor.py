"""PDF splitting and LLM extraction."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Optional, Type

import pymupdf
from pydantic import BaseModel
from rich.progress import Progress, TaskID

from infra.content import PageExtraction
from infra.llm import GeminiFilesClient, GeminiInteractionsClient, GeminiRuntime
from infra.llm.types import InteractionTurn, UriMediaContent

from .constants import GEMINI_MODEL
from .prompts import EXTRACTION_SLICES
from .queues import grow_total, iter_queue
from .types import ExtractedPage, PageMeta


class Extractor:
    """Splits input PDFs and extracts content via Gemini."""

    def __init__(self, runtime: GeminiRuntime) -> None:
        """Initialize extractor with shared Gemini runtime."""
        self._interactions = GeminiInteractionsClient(runtime)
        self._files = GeminiFilesClient(runtime)

    async def run(
        self,
        pdf_queue: asyncio.Queue[Optional[Path]],
        page_queue: asyncio.Queue[Optional[ExtractedPage]],
        done: set[str],
        progress: Progress,
        task: TaskID,
    ) -> None:
        """Consume PDFs from queue, extract new pages, push to page queue."""
        async for pdf_path in iter_queue(pdf_queue):
            pages = await self._split_new(pdf_path, done)
            grow_total(progress, task, len(pages) * len(EXTRACTION_SLICES))
            for meta, page_bytes in pages:
                extracted = await self._extract_one(meta, page_bytes, progress, task)
                await page_queue.put(extracted)

        await page_queue.put(None)

    # --- PDF splitting ---

    async def _split_new(self, pdf_path: Path, done: set[str]) -> list[tuple[PageMeta, bytes]]:
        """Split one PDF and filter out already-processed pages."""
        return [
            (meta, page_bytes)
            for meta, page_bytes in await self._split_one(pdf_path)
            if meta.page_key not in done
        ]

    async def _split_one(self, pdf_path: Path) -> list[tuple[PageMeta, bytes]]:
        """Split one PDF into single-page PDFs as bytes (async wrapper)."""
        return await asyncio.to_thread(self._split_sync, pdf_path)

    def _split_sync(self, pdf_path: Path) -> list[tuple[PageMeta, bytes]]:
        """Synchronously split one PDF into per-page byte arrays."""
        doc = pymupdf.open(pdf_path)
        results: list[tuple[PageMeta, bytes]] = []

        for i in range(len(doc)):
            single = pymupdf.open()
            single.insert_pdf(doc, from_page=i, to_page=i)
            page_bytes = single.tobytes()
            single.close()
            results.append((self._meta_from_path(pdf_path, i + 1), page_bytes))

        doc.close()
        return results

    def _meta_from_path(self, pdf_path: Path, page: int) -> PageMeta:
        """Construct PageMeta from PDF path and page number."""
        return PageMeta(
            grade=int(pdf_path.parent.parent.parent.name),
            subject=pdf_path.parent.parent.name,
            book=pdf_path.parent.name,
            chapter=pdf_path.stem,
            page=page,
        )

    # --- LLM extraction ---

    async def _extract_one(
        self,
        meta: PageMeta,
        pdf_bytes: bytes,
        progress: Progress,
        task: TaskID,
    ) -> ExtractedPage:
        """Extract LLM content from a single page PDF using parallel calls."""
        upload = await self._files.upload_bytes(pdf_bytes, "application/pdf")
        try:
            partials = await asyncio.gather(
                *(self._extract_slice(upload.uri, s.prompt, s.response, progress, task)
                  for s in EXTRACTION_SLICES)
            )
            merged: dict[str, object] = {}
            for partial in partials:
                merged.update(partial.model_dump())
            return ExtractedPage(
                meta=meta, pdf_bytes=pdf_bytes, extraction=PageExtraction(**merged),
            )
        finally:
            await self._files.delete_file(upload)

    async def _extract_slice[T: BaseModel](
        self,
        uri: str,
        prompt: str,
        schema: Type[T],
        progress: Progress,
        task: TaskID,
    ) -> T:
        """Run one typed extraction call for a single category."""
        parsed, _ = await self._interactions.chat(
            model=GEMINI_MODEL,
            system_instruction=prompt,
            input=[InteractionTurn(
                role="user",
                content=[UriMediaContent(type="document", uri=uri, mime_type="application/pdf")],
            )],
            response_schema=schema,
        )
        progress.advance(task)
        return parsed
