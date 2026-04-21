"""Publish from the local cache via GCS bulk import: reset -> stage -> import -> attach."""

from __future__ import annotations

import asyncio
import uuid
from collections import defaultdict
from pathlib import Path

from infra.constants import Const
from infra.content import METADATA_SCHEMA
from infra.rag import CorpusKind, ImportReceipt, MetadataValue, VertexRagWriter
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
        """reset -> ensure -> stage -> import -> attach over the closed cache."""
        await self._reset(reporter.reset)
        # Schemas must exist (and be visible) before attach; ensure before import.
        await self._rag.ensure_corpora(METADATA_SCHEMA)
        # Materialize once: exact totals + chapter grouping up front.
        cached = list(cache.iter_all())
        shards = await self._stage(cached, reporter.stage)
        await self._import_and_attach(shards, reporter.import_, reporter.attach)

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
        bins: dict[tuple[CorpusKind, int], list[PublishUnit]] = defaultdict(list)
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
                    bins[(unit.corpus, shard_id)].append(unit)
                    tg.create_task(self._stage_one(unit, shard_id, data, reporter))
        return [self._shard(corpus, shard_id, units)
                for (corpus, shard_id), units in sorted(bins.items())]

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

    # --- Import + attach: one LRO per shard, receipts drive attach ---

    async def _import_and_attach(
        self,
        shards: list[ImportShard],
        import_reporter: StageReporter,
        attach_reporter: StageReporter,
    ) -> None:
        """Run every shard's import->attach as an isolated unit of work; fail-fast."""
        import_reporter.grow(len(shards))
        attach_reporter.grow(sum(len(s.units) for s in shards))
        # Concurrency cap lives on VertexRagWriter (3 LROs in flight per Vertex quota).
        async with asyncio.TaskGroup() as tg:
            for shard in shards:
                tg.create_task(self._run_shard(shard, import_reporter, attach_reporter))

    async def _run_shard(
        self, shard: ImportShard,
        import_reporter: StageReporter, attach_reporter: StageReporter,
    ) -> None:
        """Import one shard, parse its receipts, attach metadata to the imported files."""
        await self._rag.import_shard(shard.corpus, shard.prefix.uri, shard.result_sink.uri)
        import_reporter.advance()
        ndjson = await self._bucket.download(shard.result_sink.object_name)
        receipts = ImportReceipt.parse_ndjson(ndjson)
        await self._attach_shard(shard, receipts, attach_reporter)

    async def _attach_shard(
        self, shard: ImportShard, receipts: list[ImportReceipt],
        reporter: StageReporter,
    ) -> None:
        """Join receipts back to units by object basename; attach metadata concurrently."""
        meta_by_object = {u.object_basename: u.metadata for u in shard.units}
        async with asyncio.TaskGroup() as tg:
            for r in receipts:
                tg.create_task(self._attach_one(r, meta_by_object, reporter))

    async def _attach_one(
        self, receipt: ImportReceipt,
        meta_by_object: dict[str, dict[str, MetadataValue]],
        reporter: StageReporter,
    ) -> None:
        """Attach one file's metadata; failed receipts crash the shard loudly."""
        await self._rag.attach_metadata(receipt.file_id, meta_by_object[receipt.object_basename])
        reporter.advance()

    # --- GCS address helpers ---

    def _shard(
        self, corpus: CorpusKind, shard_id: int, units: list[PublishUnit],
    ) -> ImportShard:
        """Bind (corpus, shard_id, units) into an ImportShard with its GCS paths."""
        return ImportShard(
            corpus=corpus,
            prefix=self._shard_prefix(corpus, shard_id),
            result_sink=self._result_sink(corpus, shard_id),
            units=units,
        )

    def _object_name(self, unit: PublishUnit, shard_id: int) -> str:
        """GCS object name for one unit under its shard directory."""
        return f"{self._shard_object_prefix(unit.corpus, shard_id)}{unit.object_basename}"

    def _shard_prefix(self, corpus: CorpusKind, shard_id: int) -> GcsPath:
        """Directory prefix the import LRO consumes (trailing slash = recurse)."""
        return GcsPath(
            bucket=self._bucket.name,
            object_name=self._shard_object_prefix(corpus, shard_id),
        )

    def _result_sink(self, corpus: CorpusKind, shard_id: int) -> GcsPath:
        """NDJSON receipt path Vertex writes to; kept outside the import prefix."""
        return GcsPath(
            bucket=self._bucket.name,
            object_name=f"runs/{self._run_id}/results/{corpus}-{shard_id:03d}.ndjson",
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
