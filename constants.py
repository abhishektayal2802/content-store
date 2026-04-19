"""Shared constants for the content_store pipeline."""

import re
from pathlib import Path

# --- Paths ---

INPUTS_ROOT: Path = Path(__file__).parent / "inputs"
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
# Retries handed to pypdl per book zip.
ZIP_RETRIES: int = 5

# --- Pipeline constants ---

QUEUE_SIZE: int = 64

# MIME -> GCS object-name extension. The import LRO uses the extension on
# the GCS object to select a chunker (PDF vs markdown vs text).
SUFFIX_BY_MIME: dict[str, str] = {
    "application/pdf": ".pdf",
    "text/markdown": ".md",
}
