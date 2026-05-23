"""Shared constants for the content_store pipeline."""

import re
from pathlib import Path

from infra.content import ARTEFACT_KINDS, QUESTION_KINDS
from infra.llm import Models, ReasoningEfforts, Verbosities
from infra.llm.constants import RESPONSE_CONCURRENCY_LIMIT

# --- Extraction LLM defaults (gpt-5.4-nano + medium reasoning) ---

EXTRACTION_MODEL = Models.SMALL
EXTRACTION_REASONING_EFFORT = ReasoningEfforts.MEDIUM
EXTRACTION_VERBOSITY = Verbosities.LOW
EXTRACTION_QUEUE_SIZE = RESPONSE_CONCURRENCY_LIMIT * 2

# --- Local code/data paths ---

CATALOG_PATH: Path = Path(__file__).parent / "catalog.json"

# --- NCERT source ---

NCERT_BASE = "https://ncert.nic.in/"
CATALOG_URL = f"{NCERT_BASE}textbook.php"
USER_AGENT = "Mozilla/5.0 (compatible; sujho-content-store/1.0)"

# Per-book zip bundle served by NCERT. "dd" suffix is NCERT convention.
BOOK_ZIP_URL_TEMPLATE = f"{NCERT_BASE}textbook/pdf/{{code}}dd.zip"
# Direct chapter PDF when a zip entry is empty (known NCERT bundle quirk).
CHAPTER_PDF_URL_TEMPLATE = f"{NCERT_BASE}textbook/pdf/{{entry}}"

# Catalog JS patterns — used only by refresh_catalog.py, not the main pipeline.
BOOK_GROUP_PATTERN = re.compile(
    r'(?:if|else if)\s*\(\(document\.test\.tclass\.value==(\d+)\)\s*&&\s*'
    r'\(document\.test\.tsubject\.options\[sind\]\.text=="([^"]+)"\)\)\s*\{(.*?)\}',
    re.S,
)
BOOK_OPTION_PATTERN = re.compile(
    r'document\.test\.tbook\.options\[(\d+)\]\.text="([^"]*)";?\s*'
    r'document\.test\.tbook\.options\[\1\]\.value="textbook\.php\?([^=]+)=([^"]+)"',
    re.S,
)

# --- Scraper constants ---

# Simultaneous book-zip downloads. NCERT is flaky, keep this low.
ZIP_CONCURRENCY: int = 3

# NCERT zip entries: "<book.code><suffix>.pdf". Chapter stems are normalized
# to human-readable names (e.g. "chapter-03") so retrieval filters are
# predictable across books. Unknown suffixes pass through unchanged.
NCERT_CHAPTER_NUM_RE = re.compile(r"^\d{2}$")
NCERT_APPENDIX_RE = re.compile(r"^a(\d+)$")
NCERT_ANNEXURE_RE = re.compile(r"^ax(\d*)$")
NCERT_LITERAL_STEMS: dict[str, str] = {
    "ps": "prelims",
    "an": "answers",
    "gl": "glossary",
    "glo": "glossary",
}

# --- Pipeline constants ---

CONTENT_STORE_RUN_ID_ENV: str = "CONTENT_STORE_RUN_ID"
CLOUD_RUN_TASK_INDEX_ENV: str = "CLOUD_RUN_TASK_INDEX"
CLOUD_RUN_TASK_COUNT_ENV: str = "CLOUD_RUN_TASK_COUNT"

RAW_PREFIX: str = "raw"
EXTRACTED_PREFIX: str = "extracted"
RUNS_PREFIX: str = "runs"
STAGING_PREFIX: str = "staging"

TELEMETRY_FLUSH_UNITS: int = 5000

# Publish order for extracted non-page units.
PUBLISH_ITEM_KINDS = QUESTION_KINDS + ARTEFACT_KINDS

# Staged transport shapes for page PDFs vs extracted markdown items.
PAGE_UNIT_SUFFIX: str = ".pdf"
PAGE_UNIT_CONTENT_TYPE: str = "application/pdf"
ITEM_UNIT_SUFFIX: str = ".md"
ITEM_UNIT_CONTENT_TYPE: str = "text/markdown"
JSONL_CONTENT_TYPE: str = "application/jsonl"
