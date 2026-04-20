"""NCERT textbook scraper: per-book zip downloads, PDF extraction, resume marker."""

from __future__ import annotations

import asyncio
import json
import zipfile
from pathlib import Path
from typing import Optional

from infra.http import download_file
from infra.text import slugify

from .constants import (
    BOOK_DONE_MARKER,
    BOOK_ZIP_URL_TEMPLATE,
    CATALOG_PATH,
    INPUTS_ROOT,
    NCERT_ANNEXURE_RE,
    NCERT_APPENDIX_RE,
    NCERT_CHAPTER_NUM_RE,
    NCERT_LITERAL_STEMS,
    USER_AGENT,
    ZIP_CACHE_ROOT,
    ZIP_CONCURRENCY,
)
from .reporter import StageReporter
from .types import Book


class Scraper:
    """Downloads per-book NCERT zips and unpacks their PDFs. Resumable[Book, str]."""

    def __init__(self) -> None:
        self._semaphore = asyncio.Semaphore(ZIP_CONCURRENCY)

    # --- Resumable[Book, str] ---

    def key(self, book: Book) -> str:
        """Book-level identity for the resume check."""
        return book.code

    async def completed_keys(self) -> set[str]:
        """Codes of every book whose inputs dir carries a `.done` marker."""
        return {
            book.code for book in self._load_catalog()
            if (self._book_dir(book) / BOOK_DONE_MARKER).exists()
        }

    async def run(
        self,
        pdf_queue: asyncio.Queue[Optional[Path]],
        reporter: StageReporter,
    ) -> None:
        """Scrape missing books, enqueue every book's PDFs, signal end-of-stream."""
        books = self._load_catalog()
        reporter.grow(len(books))
        try:
            await asyncio.gather(*(self._process(b, pdf_queue, reporter) for b in books))
        finally:
            await pdf_queue.put(None)

    # --- Catalog ---

    def _load_catalog(self) -> list[Book]:
        """Read the checked-in catalog manifest into validated Book objects."""
        raw = json.loads(CATALOG_PATH.read_text())
        return [Book(**entry) for entry in raw]

    # --- Per-book work ---

    async def _process(
        self,
        book: Book,
        pdf_queue: asyncio.Queue[Optional[Path]],
        reporter: StageReporter,
    ) -> None:
        """Ensure a book is scraped (skip via `.done` marker), enqueue its PDFs."""
        book_dir = self._book_dir(book)
        try:
            if not (book_dir / BOOK_DONE_MARKER).exists():
                await self._download_and_extract(book, book_dir)
                (book_dir / BOOK_DONE_MARKER).touch()
            for pdf_path in sorted(book_dir.glob("*.pdf")):
                await pdf_queue.put(pdf_path)
            reporter.advance()
        except Exception as e:
            reporter.record_error(self._zip_url(book), e)

    async def _download_and_extract(self, book: Book, book_dir: Path) -> list[Path]:
        """Download the book's zip (cached) and unzip its PDFs into book_dir."""
        zip_path = ZIP_CACHE_ROOT / f"{book.code}.zip"
        async with self._semaphore:
            await download_file(
                self._zip_url(book),
                zip_path,
                headers={"User-Agent": USER_AGENT},
            )
        return self._extract_pdfs(zip_path, book_dir, book)

    def _extract_pdfs(self, zip_path: Path, book_dir: Path, book: Book) -> list[Path]:
        """Extract each *.pdf from the zip into book_dir with a normalized stem."""
        book_dir.mkdir(parents=True, exist_ok=True)
        extracted: list[Path] = []
        with zipfile.ZipFile(zip_path) as zf:
            # Zips occasionally include JPG covers / READMEs -- keep only PDFs.
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

    def _normalize_chapter_stem(self, zip_entry: str, book_code: str) -> str:
        """Turn an NCERT zip entry like 'iebe101.pdf' into 'chapter-01'."""
        raw = Path(zip_entry).stem
        suffix = raw[len(book_code):] if raw.startswith(book_code) else raw
        if NCERT_CHAPTER_NUM_RE.fullmatch(suffix):
            return f"chapter-{suffix}"
        if literal := NCERT_LITERAL_STEMS.get(suffix):
            return literal
        if m := NCERT_APPENDIX_RE.fullmatch(suffix):
            return f"appendix-{int(m.group(1)):02d}"
        if m := NCERT_ANNEXURE_RE.fullmatch(suffix):
            tail = f"-{int(m.group(1)):02d}" if m.group(1) else ""
            return f"annexure{tail}"
        # Unknown suffix: preserve raw identifier so nothing is silently dropped.
        return raw
