"""Pipeline-specific types for the content_store."""

from typing import Literal, Type

from pydantic import BaseModel

from infra.content import PageExtraction, PageMeta
from infra.prompts import join_sections


# Pipeline stage labels for progress bars and error rows.
# `scrape` + `extract` run together as the streaming phase; the rest are the publish phase.
Stage = Literal["scrape", "extract", "reset", "upload", "attach"]


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
