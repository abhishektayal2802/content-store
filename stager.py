"""Stream extracted pages to GCS and build per-corpus import manifests.

Pure I/O to GCS. No chunking, no embedding, no indexing -- those happen
later in the import LRO. `stage` advances one unit per uploaded object,
so the progress bar reflects honest bytes-out-the-door.
"""

from __future__ import annotations

import asyncio
from typing import Iterator, Optional

from infra.content import (
    ARTEFACT_KINDS,
    CORPUS_BY_KIND,
    ContentMarkdownRenderer,
    ExtractedPage,
    QUESTION_KINDS,
)
from infra.rag import VertexRagClient

from .constants import SUFFIX_BY_MIME
from .queues import iter_queue
from .reporter import ProgressReporter
from .types import CorpusManifest, StagedFile, StagingUnit


class Stager:
    """Consumes extracted pages, uploads bytes to GCS, records manifest entries."""

    def __init__(self, rag: VertexRagClient) -> None:
        """Bind to a shared RAG client."""
        self._rag = rag
        self._renderer = ContentMarkdownRenderer()
        self.manifest: CorpusManifest = {"pages": [], "questions": [], "artefacts": []}

    async def run(
        self,
        page_queue: asyncio.Queue[Optional[ExtractedPage]],
        reporter: ProgressReporter,
    ) -> None:
        """Consume pages until the queue sentinel; fan out uploads in parallel."""
        tasks = []
        async for page in iter_queue(page_queue):
            units = list(self._units_for(page))
            reporter.grow("stage", len(units))
            for unit in units:
                tasks.append(asyncio.create_task(self._stage(unit, reporter)))
        await asyncio.gather(*tasks)

    async def _stage(self, unit: StagingUnit, reporter: ProgressReporter) -> None:
        """Upload one unit's bytes to GCS and append to the corpus manifest."""
        # Object path is content-addressable by display_name; re-runs overwrite
        # in-place and the bucket's lifecycle rule ages out stale objects.
        object_name = f"{unit.corpus}/{unit.display_name}{SUFFIX_BY_MIME[unit.mime]}"
        try:
            uri = await self._rag.stage(object_name, unit.content, unit.mime)
            # Single event loop -- list.append is atomic, no lock needed.
            self.manifest[unit.corpus].append(StagedFile(
                gcs_uri=uri, display_name=unit.display_name, metadata=unit.metadata,
            ))
            reporter.advance("stage")
        except Exception as e:
            reporter.record_error("stage", unit.display_name, e)

    def _units_for(self, page: ExtractedPage) -> Iterator[StagingUnit]:
        """Yield a StagingUnit for the page PDF + every extracted item."""
        base_meta = page.meta.model_dump()
        # 1) The page PDF goes to the `pages` corpus as-is.
        yield StagingUnit(
            corpus="pages",
            display_name=page.meta.display_name("pages"),
            mime="application/pdf",
            content=page.pdf_bytes,
            metadata={**base_meta, "kind": "pages"},
        )
        # 2) One markdown unit per extracted item (question or artefact).
        #    Questions carry `difficulty`; artefacts don't.
        for kind in QUESTION_KINDS + ARTEFACT_KINDS:
            corpus = CORPUS_BY_KIND[kind]
            for i, item in enumerate(getattr(page.extraction, kind), 1):
                metadata = {**base_meta, "kind": kind}
                if kind in QUESTION_KINDS:
                    metadata["difficulty"] = item.difficulty
                yield StagingUnit(
                    corpus=corpus,
                    display_name=f"{page.meta.display_name(kind)}__item-{i:03d}",
                    mime="text/markdown",
                    content=self._renderer.render(item).encode("utf-8"),
                    metadata=metadata,
                )
