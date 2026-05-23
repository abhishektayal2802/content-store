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

    def __init__(self, rag: VertexRagWriter, storage: ContentStoreStorage, run_id: str) -> None:
        self._rag = rag
        self._storage = storage
        self._run_id = run_id
        self._units = UnitBuilder()

    async def run(self, stage: StageRun) -> None:
        """Stage every unit, then reset/import Vertex only after staging succeeds."""
        raw_chapters = await self._storage.list_raw_chapters()
        if not raw_chapters:
            raise RuntimeError("no raw chapters found")
        await stage.start(0)
        await stage.activity("staging_publish_units")
        shards = await self._stage(raw_chapters, stage)
        await stage.activity("deleting_corpora")
        await self._rag.delete_all_corpora()
        await stage.activity("creating_corpora")
        await self._rag.ensure_corpora()
        await stage.activity("importing_shards")
        await self._import_shards(shards)

    # --- Stage: upload every unit's bytes to its (corpus, shard) GCS prefix ---

    async def _stage(self, chapters: list[RawChapter], stage: StageRun) -> list[ImportShard]:
        """Stage all Vertex import units without mutating existing corpora."""
        counts: dict[CorpusKind, int] = defaultdict(int)
        shard_keys: set[tuple[CorpusKind, int]] = set()
        async with asyncio.TaskGroup() as tg:
            for chapter in sorted(chapters, key=lambda c: (c.grade, c.subject, c.book, c.chapter)):
                for unit, data in await self._prepare_chapter(chapter, stage):
                    # Sequential fill: new shard every MAX_FILES_PER_SHARD units per corpus.
                    shard_id = counts[unit.corpus] // MAX_FILES_PER_SHARD
                    counts[unit.corpus] += 1
                    shard_keys.add((unit.corpus, shard_id))
                    object_name = self._storage.staging_object_name(
                        self._run_id,
                        unit.corpus,
                        shard_id,
                        unit.object_basename,
                    )
                    tg.create_task(self._stage_one(object_name, data, unit.content_type, stage))
        return [
            self._storage.import_shard(self._run_id, corpus, shard_id)
            for corpus, shard_id in sorted(shard_keys)
        ]

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

    async def _import_shards(self, shards: list[ImportShard]) -> None:
        """Run every shard import as an isolated unit of work; fail-fast."""
        # Concurrency cap lives on VertexRagWriter (3 LROs in flight per Vertex quota).
        async with asyncio.TaskGroup() as tg:
            for shard in shards:
                tg.create_task(self._rag.import_shard(shard.corpus, shard.prefix.uri))
