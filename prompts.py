"""Extraction prompts for the content_store LLM extraction pass."""

from infra.utils.prompts import join_sections

EXTRACTION_PROMPT = join_sections(
    "Extract all questions and artefacts from this textbook page.",
    "Use the response schema to classify each item into the appropriate list.",
    "Rules:",
    "- Extract only what is explicitly present on the page.\n"
    "- Do not invent, infer, merge, or normalize away important details.\n"
    "- If nothing is found for a category, return an empty list.",
)
