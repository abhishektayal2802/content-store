"""Pipeline-specific Pydantic models for the content_store."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

from infra.content import ContentRef, PageExtraction, PageMeta
from infra.rag import CorpusKind, build_rag_display_name
from infra.platform.storage import GcsPath
from infra.utils.text import slugify


ContentStoreStage = Literal["refresh", "scrape", "extract", "stage", "publish"]
StageStatus = Literal["running", "succeeded", "failed"]


class Book(BaseModel):
    """One NCERT book entry from the catalog manifest."""

    grade: int
    subject: str
    title: str
    # NCERT asset code (e.g. "iebe1") used to derive the dd.zip URL.
    code: str

    @property
    def ref(self) -> ContentRef:
        """Book provenance ref: stable slug id + the non-derivable display title."""
        return ContentRef(id=slugify(self.title), title=self.title)


class CachedPage(BaseModel):
    """One page's durable extraction record: the extract-resume unit of truth.

    Persisted as JSON under the GCS extracted prefix. PDF bytes are intentionally
    not stored here; they are re-materialized from the raw chapter PDF.
    """

    meta: PageMeta
    extraction: PageExtraction


class RawChapter(BaseModel):
    """One durable raw chapter PDF mirrored from NCERT into GCS."""

    grade: int
    subject: str
    book: ContentRef
    chapter: str
    object_name: str


class StageManifest(BaseModel):
    """One stage's compact progress counters."""

    run_id: str
    stage: ContentStoreStage
    status: StageStatus
    total: int
    completed: int
    skipped: int
    failed: int
    activity: str
    task_index: int
    task_count: int
    started_at: str
    updated_at: str


class RunError(BaseModel):
    """One structured failure record for a stage-task JSONL object."""

    run_id: str
    stage: ContentStoreStage
    context: str
    error_type: str
    message: str
    task_index: int
    timestamp: str


class PublishUnit(BaseModel):
    """Smallest filter-preserving retrieval unit: one RagFile to publish.

    `source_id` is the retrieval contract (= SourceRef.to_source_id). Vertex
    exposes the staged file as `RagFile.display_name = object_basename` after
    a GCS import, so retrieval strips `suffix` to recover the pure source id.
    """

    corpus: CorpusKind
    source_id: str
    suffix: str
    content_type: str

    @property
    def object_basename(self) -> str:
        """GCS object basename = source_id + suffix (e.g. "...__item-001.md")."""
        return build_rag_display_name(self.source_id, self.suffix)


class ImportShard(BaseModel):
    """Smallest remote import unit: one corpus, one GCS prefix, one LRO.

    `prefix` is the gs:// directory every unit was staged under; the import
    LRO consumes it as a single source URI.
    """

    corpus: CorpusKind
    prefix: GcsPath
