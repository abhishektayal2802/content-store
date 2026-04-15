"""Extraction prompt for the content store pipeline."""

# Fields extracted from each page (excluding "pages" which holds the raw PDF).
EXTRACTION_FIELDS: tuple[str, ...] = (
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
STORE_KINDS: tuple[str, ...] = ("pages", *EXTRACTION_FIELDS)

# Single unified extraction prompt.
EXTRACTION_PROMPT: str = """
Extract all educational content from this textbook page into the appropriate categories.

Rules:
- Extract only what is explicitly present on the page.
- Do not invent, infer, or normalize away important details.
- If nothing is found for a category, return an empty list.
""".strip()
