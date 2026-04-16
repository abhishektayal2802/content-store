"""NCERT textbook scraper: downloads per-book dd.zip bundles and extracts PDFs.

Design:
- The book catalog is *not* discovered at run time. It's a checked-in manifest
  (catalog.json) produced by refresh_catalog.py. This kills the "guessed URL"
  problem that produced hundreds of 404s.
- For each book in the manifest we download exactly one authoritative artefact:
  the `<code>dd.zip` bundle served by NCERT, via infra.http.download_file for
  resumable (HTTP Range + ETag) transfers with built-in retry.
- Only the PDFs inside each zip are kept. Their names come from NCERT itself,
  so we never fabricate filenames.
"""

from __future__ import annotations

import asyncio
import json
import re
import zipfile
from pathlib import Path
from typing import Optional

from infra.http import download_file
from infra.text import slugify

from .constants import (
    BOOK_ZIP_URL_TEMPLATE,
    CATALOG_PATH,
    INPUTS_ROOT,
    USER_AGENT,
    ZIP_CACHE_ROOT,
    ZIP_CONCURRENCY,
    ZIP_RETRIES,
)
from .reporter import ProgressReporter
from .types import Book


class Scraper:
    """Downloads per-book NCERT zip bundles and unpacks their PDFs."""

    def __init__(self) -> None:
        """Initialize the scraper with a concurrency limit for zip downloads."""
        self._semaphore = asyncio.Semaphore(ZIP_CONCURRENCY)

    async def run(
        self,
        pdf_queue: asyncio.Queue[Optional[Path]],
        reporter: ProgressReporter,
    ) -> None:
        """Load the manifest, download each book zip, stream PDF paths downstream."""
        books = self._load_catalog()
        reporter.grow("scrape", len(books))

        try:
            await asyncio.gather(
                *(self._ingest_book(book, pdf_queue, reporter) for book in books)
            )
        finally:
            # Always signal end-of-stream so downstream workers can drain.
            await pdf_queue.put(None)

    async def close(self) -> None:
        """No-op retained for pipeline API compatibility."""
        return None

    # --- Catalog ---

    def _load_catalog(self) -> list[Book]:
        """Read the checked-in catalog manifest into validated Book objects."""
        raw = json.loads(CATALOG_PATH.read_text())
        return [Book(**entry) for entry in raw]

    # --- Per-book ingestion ---

    async def _ingest_book(
        self,
        book: Book,
        pdf_queue: asyncio.Queue[Optional[Path]],
        reporter: ProgressReporter,
    ) -> None:
        """Download+unpack one book's zip and enqueue each extracted PDF path."""
        book_dir = self._book_dir(book)
        try:
            pdfs = await self._resolve_book_pdfs(book, book_dir)
            for pdf_path in pdfs:
                await pdf_queue.put(pdf_path)
            reporter.advance("scrape")
        except Exception as e:
            reporter.record_error("scrape", self._zip_url(book), e)

    async def _resolve_book_pdfs(self, book: Book, book_dir: Path) -> list[Path]:
        """Return the book's PDFs, downloading+unzipping if not already on disk."""
        # Fast path: if the book dir is already populated, reuse what's there.
        # This is how we resume across pipeline runs without re-downloading zips.
        existing = sorted(book_dir.glob("*.pdf"))
        if existing:
            return existing

        zip_path = ZIP_CACHE_ROOT / f"{book.code}.zip"
        # Cap concurrent NCERT connections; the downloader itself handles
        # resume, retry, and ETag validation.
        async with self._semaphore:
            await download_file(
                self._zip_url(book),
                zip_path,
                retries=ZIP_RETRIES,
                headers={"User-Agent": USER_AGENT},
            )
        return self._extract_pdfs(zip_path, book_dir, book)

    def _extract_pdfs(self, zip_path: Path, book_dir: Path, book: Book) -> list[Path]:
        """Extract each *.pdf from the zip into book_dir with a normalized stem."""
        book_dir.mkdir(parents=True, exist_ok=True)
        extracted: list[Path] = []
        with zipfile.ZipFile(zip_path) as zf:
            # Only PDFs — zips occasionally carry JPG covers, READMEs, etc.
            pdf_entries = [n for n in zf.namelist() if n.lower().endswith(".pdf")]
            for entry in pdf_entries:
                stem = self._normalize_chapter_stem(entry, book.code)
                dest = book_dir / f"{stem}.pdf"
                dest.write_bytes(zf.read(entry))
                extracted.append(dest)
        return sorted(extracted)

    # --- Naming ---

    def _book_dir(self, book: Book) -> Path:
        """Directory under inputs/ that holds one book's extracted PDFs."""
        return INPUTS_ROOT / str(book.grade) / book.subject / slugify(book.title)

    def _zip_url(self, book: Book) -> str:
        """NCERT per-book zip URL built from the manifest code."""
        return BOOK_ZIP_URL_TEMPLATE.format(code=book.code)

    # NCERT zip entries are named "<book.code><suffix>.pdf" where suffix
    # encodes document type: "01".."40" for chapters, plus a small set of
    # special codes. We translate known suffixes into human-readable chapter
    # stems (shared across books, so filters like `chapter="chapter-03"`
    # behave predictably) and fall back to the raw suffix for anything
    # unrecognized.
    _CHAPTER_NUM = re.compile(r"^\d{2}$")
    _APPENDIX_NUM = re.compile(r"^a(\d+)$")
    _ANNEXURE_NUM = re.compile(r"^ax(\d*)$")

    def _normalize_chapter_stem(self, zip_entry: str, book_code: str) -> str:
        """Turn an NCERT zip entry like 'iebe101.pdf' into 'chapter-01'."""
        # Strip directory prefix and extension; keep only the tail identifier.
        raw = Path(zip_entry).stem
        # Suffix = whatever follows the book code; if the entry doesn't start
        # with it, fall back to the raw stem so nothing is silently dropped.
        suffix = raw[len(book_code):] if raw.startswith(book_code) else raw

        if self._CHAPTER_NUM.fullmatch(suffix):
            return f"chapter-{suffix}"
        if suffix == "ps":
            return "prelims"
        if suffix == "an":
            return "answers"
        if m := self._APPENDIX_NUM.fullmatch(suffix):
            return f"appendix-{int(m.group(1)):02d}"
        if m := self._ANNEXURE_NUM.fullmatch(suffix):
            tail = f"-{int(m.group(1)):02d}" if m.group(1) else ""
            return f"annexure{tail}"
        if suffix in ("gl", "glo"):
            return "glossary"
        # Unknown suffix: preserve the raw identifier so nothing is ever dropped.
        return raw
