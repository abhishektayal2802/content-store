"""Pipeline-specific types for the content_store."""

from __future__ import annotations

from typing import Literal, Type

from pydantic import BaseModel

from infra.content import PageExtraction, PageMeta
from infra.prompts import join_sections
from infra.rag import CorpusKind, build_rag_display_name
from infra.storage import GcsPath


# Streaming (scrape+extract) + publish subphases (reset -> stage -> import).
Stage = Literal["scrape", "extract", "reset", "stage", "import"]


class Book(BaseModel):
    """One NCERT book entry from the catalog manifest."""

    grade: int
    subject: str
    title: str
    # NCERT asset code (e.g. "iebe1") used to derive the dd.zip URL.
    code: str


class CachedPage(BaseModel):
    """One page's durable extraction record: the extract-resume unit of truth.

    Persisted as JSON under `EXTRACTED_ROOT`. PDF bytes are intentionally *not*
    stored here; they are a deterministic function of the chapter PDF on disk
    and are re-materialized by the publisher on demand.
    """

    meta: PageMeta
    extraction: PageExtraction


class ExtractionSlice(BaseModel):
    """One extraction slice: description + Pydantic response schema."""

    description: str
    response: Type[BaseModel]

    @property
    def prompt(self) -> str:
        """Full extraction prompt = description + standing extraction rules."""
        rules = join_sections(
            "Rules:",
            "- Extract only what is explicitly present on the page.\n"
            "- Do not invent, infer, merge, or normalize away important details.\n"
            "- If nothing is found, return empty lists.",
        )
        return join_sections(self.description, rules)


class PublishUnit(BaseModel):
    """Smallest filter-preserving retrieval unit: one RagFile to publish.

    `source_id` is the retrieval contract (= SourceRef.source_id). Vertex
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
