"""Reset Vertex corpora and republish from the local extracted cache (direct RagFile upload)."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional

from infra.content import (
    ARTEFACT_KINDS,
    CORPUS_BY_KIND,
    METADATA_SCHEMA,
    QUESTION_KINDS,
    ContentKind,
    ContentMarkdownRenderer,
    PageExtraction,
    PageMeta,
)
from infra.rag import CorpusKind, MetadataValue, VertexRagWriter

from .cache import PageCache
from .constants import INPUTS_ROOT
from .pdf import split_pdf
from .reporter import ProgressReporter, StageReporter
from .types import CachedPage


@dataclass(frozen=True)
class _Unit:
    """One RagFile to upload + attach metadata for (internal publisher intermediate)."""

    corpus: CorpusKind
    display_name: str
    data: bytes
    suffix: str
    metadata: dict[str, MetadataValue]


class Publisher:
    """Orchestrates the cache -> direct Vertex rebuild as one rebuildable projection."""

    def __init__(self, rag: VertexRagWriter) -> None:
        self._rag = rag
        self._renderer = ContentMarkdownRenderer()

    async def run(self, cache: PageCache, reporter: ProgressReporter) -> None:
        """Reset remote, ensure corpora/schema, then upload+attach every cached unit."""
        await self._reset(reporter.reset)
        # Schemas must exist on every corpus before files are uploaded against them.
        await self._rag.ensure_corpora(METADATA_SCHEMA)
        # Materialize the cache once: we need the full list for totals + chapter grouping.
        cached = list(cache.iter_all())
        await self._publish_all(cached, reporter.upload, reporter.attach)

    async def _reset(self, reporter: StageReporter) -> None:
        """Nuke every corpus; publish is a full rebuild from the local cache."""
        reporter.grow(1)
        await self._rag.delete_all_corpora()
        reporter.advance()

    async def _publish_all(
        self,
        pages: list[CachedPage],
        upload_reporter: StageReporter,
        attach_reporter: StageReporter,
    ) -> None:
        """Group pages by chapter, load each PDF once, upload+attach pages concurrently."""
        # Exact totals are knowable up front because the cache is closed.
        total_units = sum(_count_units(p.extraction) for p in pages)
        upload_reporter.grow(total_units)
        attach_reporter.grow(total_units)
        # Chapter-at-a-time keeps peak memory bounded and reuses each split operation.
        for chapter_path, group in _group_by_chapter(pages).items():
            page_bytes = await split_pdf(chapter_path)
            await asyncio.gather(*[
                self._publish_page(
                    p.meta, page_bytes[p.meta.page - 1], p.extraction,
                    upload_reporter, attach_reporter,
                )
                for p in group
            ])

    async def _publish_page(
        self,
        meta: PageMeta,
        pdf_bytes: bytes,
        extraction: PageExtraction,
        upload_reporter: StageReporter,
        attach_reporter: StageReporter,
    ) -> None:
        """Upload every unit for a page concurrently; errors stay scoped to this page."""
        units = list(self._units_for(meta, pdf_bytes, extraction))
        try:
            await asyncio.gather(*(
                self._publish_unit(u, upload_reporter, attach_reporter) for u in units
            ))
        except Exception as e:
            upload_reporter.record_error(meta.page_key, e)

    async def _publish_unit(
        self,
        unit: _Unit,
        upload_reporter: StageReporter,
        attach_reporter: StageReporter,
    ) -> None:
        """Direct-upload one unit's bytes as a RagFile, then attach its metadata."""
        rag_file_name = await self._rag.upload_file(
            unit.corpus, unit.display_name, unit.data, unit.suffix,
        )
        upload_reporter.advance()
        await self._rag.attach_metadata(rag_file_name, unit.metadata)
        attach_reporter.advance()

    def _units_for(
        self,
        meta: PageMeta,
        pdf_bytes: bytes,
        extraction: PageExtraction,
    ) -> Iterator[_Unit]:
        yield self._unit(meta, "pages", pdf_bytes, ".pdf", item_index=0)
        for kind in QUESTION_KINDS + ARTEFACT_KINDS:
            for i, item in enumerate(getattr(extraction, kind), 1):
                # Difficulty is a question-only metadata field; skip for artefacts.
                difficulty = item.difficulty if kind in QUESTION_KINDS else None
                yield self._unit(
                    meta, kind,
                    self._renderer.render(item).encode("utf-8"), ".md",
                    item_index=i, difficulty=difficulty,
                )

    def _unit(
        self,
        meta: PageMeta,
        kind: ContentKind,
        data: bytes,
        suffix: str,
        item_index: int = 0,
        difficulty: Optional[str] = None,
    ) -> _Unit:
        """Build one upload unit; PageMeta owns the source-id (= display_name) codec."""
        return _Unit(
            corpus=CORPUS_BY_KIND[kind],
            display_name=meta.source_id(kind, item_index),
            data=data,
            suffix=suffix,
            metadata=_metadata(meta, kind, difficulty),
        )


def _count_units(extraction: PageExtraction) -> int:
    """Total units a page will produce: 1 page PDF + one per extracted item."""
    return 1 + sum(len(getattr(extraction, k)) for k in QUESTION_KINDS + ARTEFACT_KINDS)


def _metadata(
    meta: PageMeta, kind: ContentKind, difficulty: Optional[str] = None,
) -> dict[str, MetadataValue]:
    """Page meta + kind (+ difficulty for questions) as the per-unit metadata dict."""
    out: dict[str, MetadataValue] = {**meta.model_dump(), "kind": kind}
    if difficulty is not None:
        out["difficulty"] = difficulty
    return out


def _group_by_chapter(pages: list[CachedPage]) -> dict[Path, list[CachedPage]]:
    """Bucket cached pages by their source chapter PDF; preserves page order within a bucket."""
    groups: dict[Path, list[CachedPage]] = defaultdict(list)
    # Sort pages up front so each chapter's bucket is already in ascending page order.
    for page in sorted(pages, key=lambda p: (p.meta.book_key, p.meta.chapter, p.meta.page)):
        groups[_chapter_pdf_path(page)].append(page)
    return groups


def _chapter_pdf_path(page: CachedPage) -> Path:
    """Symmetric inverse of the scraper's inputs/ layout."""
    m = page.meta
    return INPUTS_ROOT / str(m.grade) / m.subject / m.book / f"{m.chapter}.pdf"
