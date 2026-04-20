"""Pipeline-specific types for the content_store."""

from typing import Literal, Type

from pydantic import BaseModel

from infra.prompts import join_sections
from infra.rag import CorpusKind, MetadataValue


# Progress reporter: pipeline stage labels for bars and error rows.
Stage = Literal["scrape", "extract", "stage", "import"]


class Book(BaseModel):
    """One NCERT book entry in the checked-in catalog manifest."""

    # CBSE grade (9..12).
    grade: int
    # Canonical slugified subject (matches ALLOWED_SUBJECTS).
    subject: str
    # Human-readable book title as it appears on ncert.nic.in.
    title: str
    # NCERT asset code (e.g. "iebe1") used to derive the dd.zip URL.
    code: str


class StagingUnit(BaseModel):
    """One file about to be uploaded to GCS for a RAG corpus.

    Destination corpus is resolved at construction time (so the stager
    doesn't need a CORPUS_BY_KIND lookup at upload time). `metadata` is
    the RAG per-file metadata dict that will be attached post-import.
    """

    corpus: CorpusKind
    display_name: str
    mime: str
    content: bytes
    metadata: dict[str, MetadataValue]


class StagedFile(BaseModel):
    """One file already in GCS, awaiting import + metadata attach.

    `metadata` is carried forward so the importer can attach it to the
    RagFile created by the import LRO.
    """

    gcs_uri: str
    display_name: str
    metadata: dict[str, MetadataValue]


# Manifest the stager hands to the importer: per-corpus list of staged files.
CorpusManifest = dict[CorpusKind, list[StagedFile]]


class ExtractionSlice(BaseModel):
    """One extraction slice: description and Pydantic response schema."""

    description: str
    response: Type[BaseModel]

    @property
    def prompt(self) -> str:
        """Build the full extraction prompt for this slice."""

        rules = join_sections(
            "Rules:",
            "- Extract only what is explicitly present on the page.\n"
            "- Do not invent, infer, merge, or normalize away important details.\n"
            "- If nothing is found, return empty lists.",
        )
        return join_sections(self.description, rules)
