"""Shared types for the content_store pipeline."""

from typing import Literal

from google.genai import types
from pydantic import BaseModel

from infra.content import PageExtraction

StoreKind = Literal[
    "pages",
    "mcq_questions",
    "very_short_questions",
    "short_questions",
    "long_questions",
    "equations",
    "code",
    "factoids",
    "definitions",
]

# Progress reporter: pipeline stage labels for bars and error rows.
Stage = Literal["scrape", "extract", "persist"]

# --- Scraper types ---


class Book(BaseModel):
    """One book discovered from the NCERT catalog."""

    grade: int
    subject: str
    title: str
    code: str
    chapter_count: int


class Asset(BaseModel):
    """One PDF to download."""

    book: Book
    filename: str
    url: str


# --- Extractor types ---


class PageMeta(BaseModel):
    """Provenance for one extracted page."""

    grade: int
    subject: str
    book: str
    chapter: str
    page: int

    @property
    def page_key(self) -> str:
        """Stable identity for incremental processing."""
        return "__".join(
            [
                f"grade-{self.grade}",
                f"subject-{self.subject}",
                f"book-{self.book}",
                f"chapter-{self.chapter}",
                f"page-{self.page}",
            ]
        )

    def display_name(self, store: str) -> str:
        """Build a deterministic display name from store kind and page metadata."""
        return f"{store}__{self.page_key}"


class ExtractedPage(BaseModel):
    """Extraction output with provenance and source PDF bytes."""

    meta: PageMeta
    pdf_bytes: bytes
    extraction: PageExtraction


# --- Persister types ---


class Document(BaseModel):
    """One file to upload to a File Search store."""

    store: StoreKind
    name: str
    content: bytes
    mime: str
    meta: PageMeta

    def upload_config(self) -> types.UploadToFileSearchStoreConfig:
        """Build the Google SDK upload config for this document."""
        return types.UploadToFileSearchStoreConfig(
            display_name=self.name,
            mime_type=self.mime,
            custom_metadata=[
                types.CustomMetadata(key="grade", numeric_value=self.meta.grade),
                types.CustomMetadata(key="subject", string_value=self.meta.subject),
                types.CustomMetadata(key="book", string_value=self.meta.book),
                types.CustomMetadata(key="chapter", string_value=self.meta.chapter),
                types.CustomMetadata(key="page", numeric_value=self.meta.page),
            ],
        )
