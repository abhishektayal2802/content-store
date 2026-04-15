"""Extraction prompts and slice definitions for the content store pipeline."""

from infra.content import (
    CodeExtraction,
    DefinitionExtraction,
    EquationExtraction,
    FactoidExtraction,
    LongExtraction,
    McqExtraction,
    ShortExtraction,
    VeryShortExtraction,
)
from .types import ExtractionSlice


EXTRACTION_SLICES: tuple[ExtractionSlice, ...] = (
    ExtractionSlice(
        field="mcq_questions",
        description="Extract only multiple-choice questions from this textbook page.\n"
                    "Return them in the `mcq_questions` list.",
        response=McqExtraction,
    ),
    ExtractionSlice(
        field="very_short_questions",
        description="Extract only very short questions from this textbook page.\n"
                    "These are questions answered with one word, phrase, line, or step.\n"
                    "Return them in the `very_short_questions` list.",
        response=VeryShortExtraction,
    ),
    ExtractionSlice(
        field="short_questions",
        description="Extract only short questions from this textbook page.\n"
                    "These are questions answered in 2-4 sentences or one compact paragraph.\n"
                    "Return them in the `short_questions` list.",
        response=ShortExtraction,
    ),
    ExtractionSlice(
        field="long_questions",
        description="Extract only long questions from this textbook page.\n"
                    "These require multi-part reasoning, derivations, or extended explanations.\n"
                    "Return them in the `long_questions` list.",
        response=LongExtraction,
    ),
    ExtractionSlice(
        field="equations",
        description="Extract only equations from this textbook page.\n"
                    "Mathematical or scientific equations with a short title.\n"
                    "Return them in the `equations` list.",
        response=EquationExtraction,
    ),
    ExtractionSlice(
        field="code",
        description="Extract only code from this textbook page.\n"
                    "Source code or pseudocode snippets with a short title.\n"
                    "Return them in the `code` list.",
        response=CodeExtraction,
    ),
    ExtractionSlice(
        field="factoids",
        description="Extract only factoids from this textbook page.\n"
                    "Atomic facts or compact sets of closely related facts with a short title.\n"
                    "Return them in the `factoids` list.",
        response=FactoidExtraction,
    ),
    ExtractionSlice(
        field="definitions",
        description="Extract only definitions from this textbook page.\n"
                    "Formal definitions whose title is typically the defined term.\n"
                    "Return them in the `definitions` list.",
        response=DefinitionExtraction,
    ),
)

# Persister stores: "pages" (raw PDFs) plus one per extraction slice.
STORE_KINDS: tuple[str, ...] = (
    "pages",
    *(s.field for s in EXTRACTION_SLICES),
)
