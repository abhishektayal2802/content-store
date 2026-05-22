"""GCS object naming and I/O for content_store durable state."""

from __future__ import annotations

from infra.content import PageMeta
from infra.platform.storage import GcsBucket, GcsPath
from infra.rag import CorpusKind
from infra.utils.text import slugify
from pydantic import BaseModel

from .constants import (
    EXTRACTED_PREFIX,
    JSONL_CONTENT_TYPE,
    RAW_PREFIX,
    RUNS_PREFIX,
    STAGING_PREFIX,
)
from .types import (
    Book,
    CachedPage,
    ImportShard,
    RawChapter,
    RunError,
    StageManifest,
)


class ContentStoreStorage:
    """Typed content-store state on top of one GCS bucket."""

    def __init__(self, bucket: GcsBucket) -> None:
        self._bucket = bucket

    # --- Raw chapters ---

    async def raw_chapter_exists(self, book: Book, chapter: str) -> bool:
        """True when one raw chapter PDF already exists in GCS."""
        return await self._bucket.exists(self.raw_chapter_object_name(book, chapter))

    async def upload_raw_chapter(self, book: Book, chapter: str, data: bytes) -> None:
        """Persist one raw chapter PDF mirrored from NCERT."""
        await self._bucket.upload(self.raw_chapter_object_name(book, chapter), data, "application/pdf")

    async def download_raw_chapter(self, chapter: RawChapter) -> bytes:
        """Read one raw chapter PDF from GCS."""
        return await self._bucket.download(chapter.object_name)

    async def list_raw_chapters(self) -> list[RawChapter]:
        """List every mirrored raw chapter PDF."""
        names = await self._bucket.list_prefix(f"{RAW_PREFIX}/")
        return [
            self._raw_chapter_from_name(name)
            for name in names
            if name.endswith(".pdf")
        ]

    def raw_chapter_object_name(self, book: Book, chapter: str) -> str:
        """GCS object name for one raw chapter PDF."""
        return f"{RAW_PREFIX}/{book.grade}/{book.subject}/{slugify(book.title)}/{chapter}.pdf"

    # --- Extracted pages ---

    async def extracted_page_exists(self, meta: PageMeta) -> bool:
        """True when one extracted page JSON exists in GCS."""
        return await self._bucket.exists(self.extracted_page_object_name(meta))

    async def write_extracted_page(self, page: CachedPage) -> None:
        """Persist one extracted page JSON."""
        await self._upload_model(self.extracted_page_object_name(page.meta), page)

    async def read_extracted_page(self, meta: PageMeta) -> CachedPage:
        """Read one extracted page JSON."""
        data = await self._bucket.download_json(self.extracted_page_object_name(meta))
        return CachedPage.model_validate(data)

    def extracted_page_object_name(self, meta: PageMeta) -> str:
        """GCS object name for one extracted page JSON."""
        return (
            f"{EXTRACTED_PREFIX}/{meta.grade}/{meta.subject}/{meta.book}/"
            f"{meta.chapter}/page-{meta.page:03d}.json"
        )

    # --- Run manifests ---

    async def write_stage_manifest(self, manifest: StageManifest) -> None:
        """Persist one stage manifest."""
        filename = (
            f"{manifest.stage}.json"
            if manifest.task_count == 1
            else f"{manifest.stage}-{manifest.task_index:05d}.json"
        )
        await self._upload_model(self._run_object(manifest.run_id, filename), manifest)

    async def append_run_error(self, error: RunError) -> None:
        """Append one structured error line to the run's JSONL error object."""
        object_name = self._run_object(
            error.run_id,
            f"errors-{error.stage}-{error.task_index:05d}.jsonl",
        )
        existing = await self._bucket.download(object_name) if await self._bucket.exists(object_name) else b""
        line = error.model_dump_json() + "\n"
        await self._bucket.upload(object_name, existing + line.encode("utf-8"), JSONL_CONTENT_TYPE)

    # --- Vertex staging ---

    async def stage_unit(self, object_name: str, data: bytes, content_type: str) -> None:
        """Upload one publish unit under the run-scoped staging prefix."""
        await self._bucket.upload(object_name, data, content_type)

    def staging_object_name(self, run_id: str, corpus: CorpusKind, shard_id: int, basename: str) -> str:
        """GCS object name for one staged Vertex import file."""
        return f"{self._staging_prefix(run_id, corpus, shard_id)}{basename}"

    def import_shard(self, run_id: str, corpus: CorpusKind, shard_id: int) -> ImportShard:
        """Typed import shard pointing at one staged prefix."""
        return ImportShard(
            corpus=corpus,
            prefix=GcsPath(
                bucket=self._bucket.name,
                object_name=self._staging_prefix(run_id, corpus, shard_id),
            ),
        )

    def _staging_prefix(self, run_id: str, corpus: CorpusKind, shard_id: int) -> str:
        """Run-scoped staging directory consumed by Vertex import."""
        return f"{RUNS_PREFIX}/{run_id}/{STAGING_PREFIX}/{corpus}-{shard_id:03d}/"

    def _run_object(self, run_id: str, filename: str) -> str:
        """Object name under one run directory."""
        return f"{RUNS_PREFIX}/{run_id}/{filename}"

    async def _upload_model(self, object_name: str, model: BaseModel) -> None:
        """Serialize one Pydantic model as a JSON object."""
        await self._bucket.upload_json(object_name, model.model_dump(mode="json"))

    def _raw_chapter_from_name(self, object_name: str) -> RawChapter:
        """Parse `raw/<grade>/<subject>/<book>/<chapter>.pdf` into a RawChapter."""
        _, grade, subject, book, filename = object_name.split("/", 4)
        return RawChapter(
            grade=int(grade),
            subject=subject,
            book=book,
            chapter=filename.removesuffix(".pdf"),
            object_name=object_name,
        )
