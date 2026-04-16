"""Pipeline-specific types for the content_store."""

from typing import Literal, Type

from pydantic import BaseModel, ConfigDict
from infra.prompts import join_sections


# Progress reporter: pipeline stage labels for bars and error rows.
Stage = Literal["scrape", "extract", "persist", "index"]


class Book(BaseModel):
    """One book discovered from the NCERT catalog."""

    grade: int
    subject: str
    title: str
    code: str
    book_url: str


class Asset(BaseModel):
    """One PDF to download."""

    book: Book
    filename: str
    url: str


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
