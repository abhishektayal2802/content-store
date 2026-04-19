"""Extraction prompts for the content_store LLM extraction pass."""

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
