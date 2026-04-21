"""Publish from the local cache via GCS bulk import: reset -> stage -> import."""

from __future__ import annotations

import asyncio
import uuid
from collections import defaultdict
from pathlib import Path

from infra.constants import Const
from infra.rag import CorpusKind, VertexRagWriter
from infra.storage import GcsBucket, GcsPath

from .cache import PageCache
from .constants import INPUTS_ROOT
from .pdf import split_pdf
from .reporter import ProgressReporter, StageReporter
from .types import CachedPage, ImportShard, PublishUnit
from .units import UnitBuilder, count_units


class Publisher:
    """Rebuild every corpus from the closed local cache via GCS bulk import."""

    def __init__(self, rag: VertexRagWriter, bucket: GcsBucket) -> None:
        self._rag = rag
        self._bucket = bucket
        self._units = UnitBuilder()
        # One run-scoped gs:// prefix for this publish; bucket TTL owns cleanup.
        self._run_id = uuid.uuid4().hex[:12]

    async def run(self, cache: PageCache, reporter: ProgressReporter) -> None:
        """reset -> ensure -> stage -> import over the closed cache."""
        await self._reset(reporter.reset)
        await self._rag.ensure_corpora()
        # Materialize once: exact totals + chapter grouping up front.
        cached = list(cache.iter_all())
        shards = await self._stage(cached, reporter.stage)
        await self._import_shards(shards, reporter.import_)

    async def _reset(self, reporter: StageReporter) -> None:
        """Nuke every corpus; publish is always a full rebuild from the cache."""
        reporter.grow(1)
        await self._rag.delete_all_corpora()
        reporter.advance()

    # --- Stage: upload every unit's bytes to its (corpus, shard) GCS prefix ---

    async def _stage(
        self, pages: list[CachedPage], reporter: StageReporter,
    ) -> list[ImportShard]:
        """Prepare chapters in parallel, then assign shards and upload every unit."""
        reporter.grow(sum(count_units(p.extraction) for p in pages))
        counts: dict[CorpusKind, int] = defaultdict(int)
        shard_keys: set[tuple[CorpusKind, int]] = set()
        chapter_units = await asyncio.gather(*[
            self._prepare_chapter(chapter_path, group)
            for chapter_path, group in _group_by_chapter(pages).items()
        ])
        async with asyncio.TaskGroup() as tg:
            for units in chapter_units:
                for unit, data in units:
                    # Sequential fill: new shard every MAX_FILES_PER_SHARD units per corpus.
                    shard_id = counts[unit.corpus] // Const.Rag.MAX_FILES_PER_SHARD
                    counts[unit.corpus] += 1
                    shard_keys.add((unit.corpus, shard_id))
                    tg.create_task(self._stage_one(unit, shard_id, data, reporter))
        return [self._shard(corpus, shard_id) for corpus, shard_id in sorted(shard_keys)]

    async def _prepare_chapter(
        self,
        chapter_path: Path,
        pages: list[CachedPage],
    ) -> list[tuple[PublishUnit, bytes]]:
        """Split one chapter once and materialize every unit's staged payload."""
        page_bytes = await split_pdf(chapter_path)
        units: list[tuple[PublishUnit, bytes]] = []
        for p in pages:
            units.extend(self._units.build(
                p.meta, page_bytes[p.meta.page - 1], p.extraction,
            ))
        return units

    async def _stage_one(
        self, unit: PublishUnit, shard_id: int, data: bytes, reporter: StageReporter,
    ) -> None:
        """Upload one unit's bytes to its shard object; advance on success, raise on fail."""
        await self._bucket.upload(
            self._object_name(unit, shard_id), data, unit.content_type,
        )
        reporter.advance()

    # --- Import: one LRO per shard ---

    async def _import_shards(
        self,
        shards: list[ImportShard],
        import_reporter: StageReporter,
    ) -> None:
        """Run every shard import as an isolated unit of work; fail-fast."""
        import_reporter.grow(len(shards))
        # Concurrency cap lives on VertexRagWriter (3 LROs in flight per Vertex quota).
        async with asyncio.TaskGroup() as tg:
            for shard in shards:
                tg.create_task(self._run_shard(shard, import_reporter))

    async def _run_shard(
        self, shard: ImportShard,
        import_reporter: StageReporter,
    ) -> None:
        """Import one shard and advance once its LRO resolves."""
        await self._rag.import_shard(shard.corpus, shard.prefix.uri)
        import_reporter.advance()

    # --- GCS address helpers ---

    def _shard(self, corpus: CorpusKind, shard_id: int) -> ImportShard:
        """Bind (corpus, shard_id) into an ImportShard with its GCS prefix."""
        return ImportShard(corpus=corpus, prefix=self._shard_prefix(corpus, shard_id))

    def _object_name(self, unit: PublishUnit, shard_id: int) -> str:
        """GCS object name for one unit under its shard directory."""
        return f"{self._shard_object_prefix(unit.corpus, shard_id)}{unit.object_basename}"

    def _shard_prefix(self, corpus: CorpusKind, shard_id: int) -> GcsPath:
        """Directory prefix the import LRO consumes (trailing slash = recurse)."""
        return GcsPath(
            bucket=self._bucket.name,
            object_name=self._shard_object_prefix(corpus, shard_id),
        )

    def _shard_object_prefix(self, corpus: CorpusKind, shard_id: int) -> str:
        """Plain object-name prefix for one shard's files (no gs://, trailing slash)."""
        return f"runs/{self._run_id}/shards/{corpus}-{shard_id:03d}/"


def _group_by_chapter(pages: list[CachedPage]) -> dict[Path, list[CachedPage]]:
    """Bucket cached pages by their source chapter PDF; preserves page order within."""
    groups: dict[Path, list[CachedPage]] = defaultdict(list)
    # Sort up front so each chapter's bucket is in ascending page order.
    for page in sorted(pages, key=lambda p: (p.meta.book_key, p.meta.chapter, p.meta.page)):
        groups[_chapter_pdf_path(page)].append(page)
    return groups


def _chapter_pdf_path(page: CachedPage) -> Path:
    """Symmetric inverse of the scraper's inputs/ layout."""
    m = page.meta
    return INPUTS_ROOT / str(m.grade) / m.subject / m.book / f"{m.chapter}.pdf"
