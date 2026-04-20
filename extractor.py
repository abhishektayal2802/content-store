"""PDF splitting and LLM extraction; persists results to the local page cache."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Optional, Type

from pydantic import BaseModel

from infra.content import PageExtraction, PageMeta
from infra.llm import GeminiFilesClient, GeminiInteractionsClient, GeminiRuntime, Models
from infra.llm.types import InteractionTurn, UriMediaContent

from .cache import PageCache
from .pdf import split_pdf
from .prompts import EXTRACTION_SLICES
from .queues import iter_queue
from .reporter import StageReporter
from .types import CachedPage


class Extractor:
    """Splits input PDFs and extracts content via Gemini; caches results to disk."""

    def __init__(self, runtime: GeminiRuntime) -> None:
        self._interactions = GeminiInteractionsClient(runtime)
        self._files = GeminiFilesClient(runtime)

    async def run(
        self,
        pdf_queue: asyncio.Queue[Optional[Path]],
        cache: PageCache,
        reporter: StageReporter,
    ) -> None:
        """Drain the scraper's PDF queue; extract + cache every not-yet-cached page."""
        tasks: list[asyncio.Task[None]] = []
        async for pdf_path in iter_queue(pdf_queue):
            # Split once per chapter PDF, then filter by cache presence (the resume signal).
            pages = await self._split_missing(pdf_path, cache)
            reporter.grow(len(pages))
            for meta, page_bytes in pages:
                tasks.append(asyncio.create_task(
                    self._extract_one(meta, page_bytes, cache, reporter)
                ))
        await asyncio.gather(*tasks)

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

    async def _extract_one(
        self,
        meta: PageMeta,
        pdf_bytes: bytes,
        cache: PageCache,
        reporter: StageReporter,
    ) -> None:
        """Extract one page end-to-end; errors are scoped so siblings keep working."""
        try:
            upload = await self._files.upload_bytes(pdf_bytes, "application/pdf")
            extraction = await self._run_slices(upload.uri)
            # Persist *before* reporting progress -- the cache file is the true checkpoint.
            cache.write(CachedPage(meta=meta, extraction=extraction))
            reporter.advance()
            # Cleanup is best-effort: Gemini TTL will reap anything we leak.
            try:
                await self._files.delete_file(upload)
            except Exception:
                pass
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
