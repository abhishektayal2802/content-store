"""Shared constants for the content_store pipeline."""

import re
from pathlib import Path

from .types import Stage

# --- Paths ---

INPUTS_ROOT: Path = Path(__file__).parent / "inputs"
# Durable per-page extraction cache; mirrors the INPUTS_ROOT layout one-to-one.
# Cache presence is the extract-stage resume signal -- no remote state involved.
EXTRACTED_ROOT: Path = Path(__file__).parent / "extracted"
CATALOG_PATH: Path = Path(__file__).parent / "catalog.json"
ZIP_CACHE_ROOT: Path = INPUTS_ROOT / "_zips"

# --- NCERT source ---

NCERT_BASE = "https://ncert.nic.in/"
CATALOG_URL = f"{NCERT_BASE}textbook.php"
USER_AGENT = "Mozilla/5.0 (compatible; sujho-content-store/1.0)"

# Per-book zip bundle served by NCERT. "dd" suffix is NCERT convention.
BOOK_ZIP_URL_TEMPLATE = f"{NCERT_BASE}textbook/pdf/{{code}}dd.zip"

ALLOWED_GRADES = {9, 10, 11, 12}
ALLOWED_SUBJECTS = {
    "accountancy",
    "biology",
    "business-studies",
    "chemistry",
    "computer-science",
    "economics",
    "english",
    "geography",
    "hindi",
    "history",
    "mathematics",
    "physics",
    "political-science",
    "psychology",
    "social-science",
    "sociology",
}

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

QUEUE_SIZE: int = 64
# Book-dir marker: present = scraped cleanly; absent = next run re-scrapes.
BOOK_DONE_MARKER: str = ".done"

# Progress-bar labels for each pipeline stage (surfaced by ProgressReporter).
# The publish-phase stages (reset/upload/attach) all have pre-computable totals
# because they operate over the closed extracted cache, not a live stream.
STAGE_LABELS: dict[Stage, str] = {
    "scrape": "Scraping PDFs",
    "extract": "Extracting pages",
    "reset": "Resetting remote state",
    "upload": "Uploading to Vertex",
    "attach": "Attaching metadata",
}
