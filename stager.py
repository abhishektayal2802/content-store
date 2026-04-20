"""Stream extracted pages to GCS; each page stages atomically, then its sentinel."""

from __future__ import annotations

import asyncio
from typing import Iterator, Optional

from infra.content import (
    ARTEFACT_KINDS,
    CORPUS_BY_KIND,
    ContentKind,
    ContentMarkdownRenderer,
    ExtractedPage,
    PageMeta,
    QUESTION_KINDS,
)
from infra.rag import VertexRagClient

from .constants import SUFFIX_BY_MIME
from .queues import iter_queue
from .reporter import StageReporter
from .types import CorpusManifest, StagedFile, StagingMetadata, StagingUnit


class Stager:
    """Consumes extracted pages, uploads bytes to GCS, records manifest entries."""

    def __init__(self, rag: VertexRagClient) -> None:
        self._rag = rag
        self._renderer = ContentMarkdownRenderer()
        self.manifest: CorpusManifest = {"pages": [], "questions": [], "artefacts": []}

    async def run(
        self,
        page_queue: asyncio.Queue[Optional[ExtractedPage]],
        reporter: StageReporter,
    ) -> None:
        """Consume pages; each page's units upload as one atomic batch."""
        tasks = []
        async for page in iter_queue(page_queue):
            tasks.append(asyncio.create_task(self._stage_page(page, reporter)))
        await asyncio.gather(*tasks)

    async def _stage_page(
        self, page: ExtractedPage, reporter: StageReporter,
    ) -> None:
        """Upload all units for a page; on success write its sentinel."""
        units = list(self._units_for(page))
        reporter.grow(len(units))
        try:
            await asyncio.gather(*(self._upload(u, reporter) for u in units))
            await self._rag.mark_done(page.meta.page_key)
        except Exception as e:
            reporter.record_error(page.meta.page_key, e)

    async def _upload(self, unit: StagingUnit, reporter: StageReporter) -> None:
        """Upload bytes + metadata to GCS; record manifest row."""
        uri = await self._rag.stage(
            unit.object_name, unit.content, unit.mime,
            metadata={k: str(v) for k, v in unit.metadata.items()},
        )
        self.manifest[unit.corpus].append(
            StagedFile(gcs_uri=uri, metadata=unit.metadata)
        )
        reporter.advance()

    def _units_for(self, page: ExtractedPage) -> Iterator[StagingUnit]:
        """Yield a StagingUnit for the page PDF + one per extracted item."""
        yield self._unit(page.meta, "pages", page.pdf_bytes, "application/pdf")
        for kind in QUESTION_KINDS + ARTEFACT_KINDS:
            for i, item in enumerate(getattr(page.extraction, kind), 1):
                difficulty = item.difficulty if kind in QUESTION_KINDS else None
                yield self._unit(
                    page.meta, kind,
                    self._renderer.render(item).encode("utf-8"), "text/markdown",
                    item_suffix=f"__item-{i:03d}", difficulty=difficulty,
                )

    def _unit(
        self,
        meta: PageMeta,
        kind: ContentKind,
        content: bytes,
        mime: str,
        item_suffix: str = "",
        difficulty: Optional[str] = None,
    ) -> StagingUnit:
        """Build one StagingUnit with its sharded GCS object name."""
        corpus = CORPUS_BY_KIND[kind]
        # Shard by book_key to stay under LRO file caps; kind in filename keeps it unique.
        object_name = (
            f"{corpus}/{meta.book_key}"
            f"/{kind}__{meta.page_key}{item_suffix}{SUFFIX_BY_MIME[mime]}"
        )
        return StagingUnit(
            corpus=corpus,
            object_name=object_name,
            mime=mime,
            content=content,
            metadata=_metadata(meta, kind, difficulty),
        )


def _metadata(meta: PageMeta, kind: ContentKind, difficulty: Optional[str] = None) -> StagingMetadata:
    """Page meta + kind (+ difficulty for questions) as the per-unit metadata dict."""
    out: StagingMetadata = {**meta.model_dump(), "kind": kind}
    if difficulty is not None:
        out["difficulty"] = difficulty
    return out
