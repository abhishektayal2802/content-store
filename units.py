"""Cached page -> (PublishUnit, raw_bytes) projection + counters.

Concentrates the "what do we publish for one page" codec so the publisher
can focus on the "how we stage/import/attach" pipeline.
"""

from __future__ import annotations

from typing import Iterator, Optional

from infra.content import (
    CORPUS_BY_KIND,
    ContentKind,
    ContentMarkdownRenderer,
    PageExtraction,
    PageMeta,
    QUESTION_KINDS,
)
from infra.rag import MetadataValue

from .constants import (
    ITEM_UNIT_CONTENT_TYPE,
    ITEM_UNIT_SUFFIX,
    PAGE_UNIT_CONTENT_TYPE,
    PAGE_UNIT_SUFFIX,
    PUBLISH_ITEM_KINDS,
)
from .types import PublishUnit


class UnitBuilder:
    """Renders one cached page into its ordered (PublishUnit, bytes) stream."""

    def __init__(self) -> None:
        self._renderer = ContentMarkdownRenderer()

    def build(
        self, meta: PageMeta, pdf_bytes: bytes, extraction: PageExtraction,
    ) -> Iterator[tuple[PublishUnit, bytes]]:
        """Yield (unit, bytes) for the page PDF + one unit per extracted item."""
        # The page PDF is a single-instance unit; item_index=0 disambiguates it.
        yield self._pair(meta, "pages", pdf_bytes, item_index=0)
        for kind in PUBLISH_ITEM_KINDS:
            for i, item in enumerate(getattr(extraction, kind), 1):
                # Difficulty is question-only metadata; artefacts skip it.
                difficulty = item.difficulty if kind in QUESTION_KINDS else None
                rendered = self._renderer.render(item).encode("utf-8")
                yield self._pair(meta, kind, rendered, item_index=i, difficulty=difficulty)

    def _pair(
        self,
        meta: PageMeta,
        kind: ContentKind,
        data: bytes,
        item_index: int,
        difficulty: Optional[str] = None,
    ) -> tuple[PublishUnit, bytes]:
        """Build (PublishUnit, bytes); PageMeta owns the source-id codec."""
        suffix, content_type = (
            (PAGE_UNIT_SUFFIX, PAGE_UNIT_CONTENT_TYPE)
            if kind == "pages"
            else (ITEM_UNIT_SUFFIX, ITEM_UNIT_CONTENT_TYPE)
        )
        unit = PublishUnit(
            corpus=CORPUS_BY_KIND[kind],
            source_id=meta.source_id(kind, item_index),
            suffix=suffix,
            content_type=content_type,
            metadata=_metadata(meta, kind, difficulty),
        )
        return unit, data


def count_units(extraction: PageExtraction) -> int:
    """Total units a page will produce: 1 page PDF + one per extracted item."""
    return 1 + sum(len(getattr(extraction, k)) for k in PUBLISH_ITEM_KINDS)


def _metadata(
    meta: PageMeta, kind: ContentKind, difficulty: Optional[str] = None,
) -> dict[str, MetadataValue]:
    """Page meta + kind (+ difficulty for questions) as the per-unit metadata dict."""
    out: dict[str, MetadataValue] = {**meta.model_dump(), "kind": kind}
    if difficulty is not None:
        out["difficulty"] = difficulty
    return out
