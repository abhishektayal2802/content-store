"""PDF splitting and LLM extraction."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Optional, Type

import pymupdf
from pydantic import BaseModel

from infra.content import PageExtraction
from infra.llm import GeminiFilesClient, GeminiInteractionsClient, GeminiRuntime, Models
from infra.llm.types import InteractionTurn, UriMediaContent

from .prompts import EXTRACTION_SLICES
from .queues import iter_queue
from .reporter import StageReporter
from infra.content import ExtractedPage, PageMeta


class Extractor:
    """Splits input PDFs and extracts content via Gemini."""

    def __init__(self, runtime: GeminiRuntime) -> None:
        self._interactions = GeminiInteractionsClient(runtime)
        self._files = GeminiFilesClient(runtime)

    async def run(
        self,
        pdf_queue: asyncio.Queue[Optional[Path]],
        page_queue: asyncio.Queue[Optional[ExtractedPage]],
        staged_page_keys: set[str],
        reporter: StageReporter,
    ) -> None:
        """Extract pages missing a GCS sentinel; push them downstream."""
        tasks = []
        async for pdf_path in iter_queue(pdf_queue):
            pages = await self._split_new(pdf_path, staged_page_keys)
            reporter.grow(len(pages))
            for meta, page_bytes in pages:
                tasks.append(asyncio.create_task(
                    self._extract_one(meta, page_bytes, page_queue, reporter)
                ))

        await asyncio.gather(*tasks)
        await page_queue.put(None)

    # --- PDF splitting ---

    async def _split_new(
        self, pdf_path: Path, staged_page_keys: set[str],
    ) -> list[tuple[PageMeta, bytes]]:
        """Split one PDF and filter out pages already fully staged in GCS."""
        return [
            (meta, page_bytes)
            for meta, page_bytes in await self._split_one(pdf_path)
            if meta.page_key not in staged_page_keys
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
        page_queue: asyncio.Queue[Optional[ExtractedPage]],
        reporter: StageReporter,
    ) -> None:
        """Extract one page; errors are scoped to this page. Leaks bounded by Gemini's TTL."""
        try:
            upload = await self._files.upload_bytes(pdf_bytes, "application/pdf")
            extraction = await self._run_slices(upload.uri)
            await page_queue.put(ExtractedPage(meta=meta, pdf_bytes=pdf_bytes, extraction=extraction))
            reporter.advance()
            await self._files.delete_file(upload)
        except Exception as e:
            reporter.record_error(meta.page_key, e)

    async def _run_slices(self, uri: str) -> PageExtraction:
        """Run all extraction slices in parallel and merge results."""
        partials = await asyncio.gather(
            *(self._extract_slice(uri, s.prompt, s.response) for s in EXTRACTION_SLICES)
        )
        return PageExtraction(**{k: v for p in partials for k, v in p.model_dump().items()})

    async def _extract_slice[T: BaseModel](
        self,
        uri: str,
        prompt: str,
        schema: Type[T],
    ) -> T:
        """Run one typed extraction call for a single category."""
        parsed, _ = await self._interactions.chat(
            model=Models.SMALL,
            system_instruction=prompt,
            input=[InteractionTurn(
                role="user",
                content=[UriMediaContent(type="document", uri=uri, mime_type="application/pdf")],
            )],
            response_schema=schema,
        )
        return parsed
