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
        """Upload every unit for one page, then write its sentinel. Page-scoped failure."""
        units = list(self._units_for(page))
        reporter.grow(len(units))
        try:
            await asyncio.gather(*(self._upload(u, reporter) for u in units))
            await self._rag.mark_done(page.meta.page_key)
        except Exception as e:
            reporter.record_error(page.meta.page_key, e)

    async def _upload(self, unit: StagingUnit, reporter: StageReporter) -> None:
        """Upload one unit's bytes to GCS and append its manifest row."""
        # Content-addressable object path: re-runs overwrite, lifecycle ages stale out.
        object_name = f"{unit.corpus}/{unit.display_name}{SUFFIX_BY_MIME[unit.mime]}"
        uri = await self._rag.stage(object_name, unit.content, unit.mime)
        self.manifest[unit.corpus].append(StagedFile(
            gcs_uri=uri, display_name=unit.display_name, metadata=unit.metadata,
        ))
        reporter.advance()

    def _units_for(self, page: ExtractedPage) -> Iterator[StagingUnit]:
        """Yield a StagingUnit for the page PDF + every extracted item."""
        yield StagingUnit(
            corpus="pages",
            display_name=page.meta.display_name("pages"),
            mime="application/pdf",
            content=page.pdf_bytes,
            metadata=_metadata(page.meta, "pages"),
        )
        for kind in QUESTION_KINDS + ARTEFACT_KINDS:
            for i, item in enumerate(getattr(page.extraction, kind), 1):
                difficulty = item.difficulty if kind in QUESTION_KINDS else None
                yield StagingUnit(
                    corpus=CORPUS_BY_KIND[kind],
                    display_name=f"{page.meta.display_name(kind)}__item-{i:03d}",
                    mime="text/markdown",
                    content=self._renderer.render(item).encode("utf-8"),
                    metadata=_metadata(page.meta, kind, difficulty),
                )


def _metadata(meta: PageMeta, kind: ContentKind, difficulty: Optional[str] = None) -> StagingMetadata:
    """Assemble the per-unit metadata dict from page meta + kind (+ difficulty)."""
    out: StagingMetadata = {**meta.model_dump(), "kind": kind}
    if difficulty is not None:
        out["difficulty"] = difficulty
    return out
