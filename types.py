"""Pipeline-specific types for the content_store."""

from typing import Literal, Type

from pydantic import BaseModel, ConfigDict
from infra.prompts import join_sections


# Progress reporter: pipeline stage labels for bars and error rows.
Stage = Literal["scrape", "extract", "persist", "index"]


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


class ExtractionSlice(BaseModel):
    """One extraction slice: description and Pydantic response schema."""

    model_config = ConfigDict(frozen=True)

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
