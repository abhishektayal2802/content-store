"""Shared constants for the content_store pipeline."""

import re
from pathlib import Path

# --- Paths ---

INPUTS_ROOT: Path = Path(__file__).parent / "inputs"

# --- Scraper constants ---

NCERT_BASE = "https://ncert.nic.in/"
CATALOG_URL = f"{NCERT_BASE}textbook.php"
USER_AGENT = "Mozilla/5.0 (compatible; sujho-content-store/1.0)"

ALLOWED_GRADES = {9, 10, 11, 12}
ALLOWED_SUBJECTS = {
    "Accountancy",
    "Biology",
    "Business Studies",
    "Chemistry",
    "Computer Science",
    "Economics",
    "English",
    "Geography",
    "Hindi",
    "History",
    "Mathematics",
    "Physics",
    "Political Science",
    "Psychology",
    "Social Science",
    "Sociology",
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

EXTRA_SUFFIXES = [
    ("prelims.pdf", "ps"),
    ("answers.pdf", "an"),
    ("appendix.pdf", "a1"),
    ("appendix-01.pdf", "a1"),
    ("appendix-02.pdf", "a2"),
]

# --- Extractor constants ---

GEMINI_MODEL = "gemini-2.5-flash-lite"

# --- Pipeline constants ---

QUEUE_SIZE: int = 64
