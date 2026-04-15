"""Extraction prompts and slice definitions for the content store pipeline."""

from infra.content import ArtefactsExtraction, QuestionsExtraction

from .types import ExtractionSlice

EXTRACTION_SLICES: tuple[ExtractionSlice, ...] = (
    ExtractionSlice(
        description=(
            "Extract all questions from this textbook page.\n"
            "Use the response schema to classify them into the appropriate question lists."
        ),
        response=QuestionsExtraction,
    ),
    ExtractionSlice(
        description=(
            "Extract all artefacts from this textbook page.\n"
            "Use the response schema to classify them into the appropriate artefact lists."
        ),
        response=ArtefactsExtraction,
    ),
)

# PageExtraction field names for persister iteration.
STORE_FIELDS: tuple[str, ...] = (
    "mcq_questions",
    "very_short_questions",
    "short_questions",
    "long_questions",
    "equations",
    "code",
    "factoids",
    "definitions",
)

# Persister stores: "pages" (raw PDFs) plus one per extraction field.
STORE_KINDS: tuple[str, ...] = ("pages", *STORE_FIELDS)
