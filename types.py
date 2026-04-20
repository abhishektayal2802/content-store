"""Pipeline-specific types for the content_store."""

from typing import Literal, Protocol, Type

from pydantic import BaseModel, ConfigDict

from infra.prompts import join_sections
from infra.rag import CorpusKind, MetadataValue


# Pipeline stage labels for progress bars and error rows.
Stage = Literal["scrape", "extract", "stage", "import"]


class Resumable[UnitT, KeyT](Protocol):
    """Resume contract: map a unit to its sentinel key + list completed keys."""

    def key(self, unit: UnitT) -> KeyT: ...

    async def completed_keys(self) -> set[KeyT]: ...


class Book(BaseModel):
    """One NCERT book entry from the catalog manifest."""

    grade: int
    subject: str
    title: str
    # NCERT asset code (e.g. "iebe1") used to derive the dd.zip URL.
    code: str


# Per-file metadata dict attached on upload + import.
StagingMetadata = dict[str, MetadataValue]


class StagingUnit(BaseModel):
    """One file queued for upload to GCS."""

    model_config = ConfigDict(frozen=True)

    corpus: CorpusKind
    object_name: str
    mime: str
    content: bytes
    metadata: StagingMetadata


class StagedFile(BaseModel):
    """One file already in GCS, awaiting import + metadata attach."""

    model_config = ConfigDict(frozen=True)

    gcs_uri: str
    metadata: StagingMetadata


# Stager -> Importer handoff.
CorpusManifest = dict[CorpusKind, list[StagedFile]]


class ExtractionSlice(BaseModel):
    """One extraction slice: description + Pydantic response schema."""

    model_config = ConfigDict(frozen=True)

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
