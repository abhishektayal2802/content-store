"""Publish from GCS extracted state via GCS bulk import into Vertex RAG."""

from __future__ import annotations

import asyncio
from collections import defaultdict

from infra.content import PageMeta
from infra.rag import MAX_FILES_PER_SHARD, CorpusKind, VertexRagWriter

from .pdf import split_pdf
from .run_state import StageRun
from .storage import ContentStoreStorage
from .types import CachedPage, ImportShard, PublishUnit, RawChapter
from .units import UnitBuilder, count_units


class Publisher:
    """Rebuild every corpus from the closed extracted GCS cache."""

    def __init__(self, storage: ContentStoreStorage, run_id: str) -> None:
        self._storage = storage
        self._run_id = run_id
        self._units = UnitBuilder()

    async def stage(self, stage: StageRun) -> None:
        """Stage every publish unit into run-scoped GCS prefixes."""
        raw_chapters = await self._storage.list_raw_chapters()
        if not raw_chapters:
            raise RuntimeError("no raw chapters found")
        await stage.start(0)
        await stage.activity("clearing_staging")
        await self._storage.delete_staging(self._run_id)
        await stage.activity("staging_publish_units")
        await self._stage(raw_chapters, stage)

    async def publish(self, rag: VertexRagWriter, stage: StageRun) -> None:
        """Reset Vertex corpora and import staged GCS shard prefixes."""
        await self._storage.require_succeeded_stage(self._run_id, "stage")
        shards = await self._storage.list_import_shards(self._run_id)
        if not shards:
            raise RuntimeError("no staged import shards found")
        await stage.start(len(shards))
        await stage.activity("deleting_corpora")
        await rag.delete_all_corpora()
        await stage.activity("creating_corpora")
        await rag.ensure_corpora()
        await stage.activity("importing_shards")
        await self._import_shards(rag, shards, stage)

    # --- Stage: upload every unit's bytes to its (corpus, shard) GCS prefix ---

    async def _stage(self, chapters: list[RawChapter], stage: StageRun) -> None:
        """Stage all Vertex import units without mutating existing corpora."""
        counts: dict[CorpusKind, int] = defaultdict(int)
        async with asyncio.TaskGroup() as tg:
            for chapter in sorted(chapters, key=lambda c: (c.grade, c.subject, c.book, c.chapter)):
                for unit, data in await self._prepare_chapter(chapter, stage):
                    # Sequential fill: new shard every MAX_FILES_PER_SHARD units per corpus.
                    shard_id = counts[unit.corpus] // MAX_FILES_PER_SHARD
                    counts[unit.corpus] += 1
                    object_name = self._storage.staging_object_name(
                        self._run_id,
                        unit.corpus,
                        shard_id,
                        unit.object_basename,
                    )
                    tg.create_task(self._stage_one(object_name, data, unit.content_type, stage))

    async def _prepare_chapter(
        self,
        chapter: RawChapter,
        stage: StageRun,
    ) -> list[tuple[PublishUnit, bytes]]:
        """Split one raw chapter and materialize every page's staged payload."""
        page_bytes = await split_pdf(await self._storage.download_raw_chapter(chapter))
        units: list[tuple[PublishUnit, bytes]] = []
        for i, data in enumerate(page_bytes, 1):
            page = await self._read_required_page(chapter, i)
            await stage.planned(count_units(page.extraction))
            units.extend(self._units.build(
                page.meta, data, page.extraction,
            ))
        return units

    async def _stage_one(
        self,
        object_name: str,
        data: bytes,
        content_type: str,
        stage: StageRun,
    ) -> None:
        """Upload one unit's bytes to its shard object; advance on success, raise on fail."""
        await self._storage.stage_unit(object_name, data, content_type)
        await stage.completed()

    async def _read_required_page(
        self,
        chapter: RawChapter,
        page: int,
    ) -> CachedPage:
        """Read one required extraction or fail before any Vertex mutation."""
        meta = PageMeta(
            grade=chapter.grade,
            subject=chapter.subject,
            book=chapter.book,
            chapter=chapter.chapter,
            page=page,
        )
        return await self._storage.read_extracted_page(meta)

    # --- Import: one LRO per shard ---

    async def _import_shards(
        self,
        rag: VertexRagWriter,
        shards: list[ImportShard],
        stage: StageRun,
    ) -> None:
        """Run every shard import as an isolated unit of work; fail-fast."""
        # Concurrency cap lives on VertexRagWriter (3 LROs in flight per Vertex quota).
        async with asyncio.TaskGroup() as tg:
            for shard in shards:
                tg.create_task(self._import_one(rag, shard, stage))

    async def _import_one(
        self,
        rag: VertexRagWriter,
        shard: ImportShard,
        stage: StageRun,
    ) -> None:
        """Import one shard and advance publish progress."""
        await rag.import_shard(shard.corpus, shard.prefix.uri)
        await stage.completed()
        await stage.checkpoint()
