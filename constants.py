"""Shared constants for the content_store pipeline."""

import re
from pathlib import Path

# --- Paths ---

INPUTS_ROOT: Path = Path(__file__).parent / "inputs"

# --- Scraper constants ---

NCERT_BASE = "https://ncert.nic.in/"
CATALOG_URL = f"{NCERT_BASE}textbook.php"
USER_AGENT = "Mozilla/5.0 (compatible; sujho-content-store/1.0)"
SCRAPE_CONCURRENCY: int = 6

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

# Regex to extract suffix values from per-book page JavaScript.
SUFFIX_PATTERN = re.compile(r'textbook\.php\?\w+=(\w+)"')

# --- Extractor constants ---

GEMINI_MODEL = "gemini-3.1-flash-lite-preview"

# --- Pipeline constants ---

QUEUE_SIZE: int = 64

# Seconds between polls while waiting for File Search indexing to complete.
INDEX_POLL_INTERVAL: float = 5.0
