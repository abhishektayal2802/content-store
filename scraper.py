"""NCERT textbook scraper: low-concurrency mirror into raw GCS PDFs."""

from __future__ import annotations

import asyncio
import tempfile
import zipfile
from pathlib import Path

from infra.platform.http import download_file, get_bytes

from .constants import (
    BOOK_ZIP_URL_TEMPLATE,
    CHAPTER_PDF_URL_TEMPLATE,
    NCERT_ANNEXURE_RE,
    NCERT_APPENDIX_RE,
    NCERT_CHAPTER_NUM_RE,
    NCERT_LITERAL_STEMS,
    USER_AGENT,
    ZIP_CONCURRENCY,
)
from .run_state import StageRun
from .storage import ContentStoreStorage
from .types import Book


class Scraper:
    """Downloads NCERT zips and uploads normalized chapter PDFs to GCS."""

    def __init__(self, storage: ContentStoreStorage, run_id: str) -> None:
        self._storage = storage
        self._run_id = run_id
        self._semaphore = asyncio.Semaphore(ZIP_CONCURRENCY)

    async def run(self, stage: StageRun) -> None:
        """Mirror every catalog book into raw GCS chapter PDFs."""
        books = await self._storage.read_catalog(self._run_id)
        await stage.start(len(books))
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            await asyncio.gather(*[
                self._process(book, root, stage)
                for book in books
            ])

    # --- Per-book mirroring ---

    async def _process(self, book: Book, root: Path, stage: StageRun) -> None:
        """Download one NCERT book zip and upload its missing chapter PDFs."""
        zip_path = root / f"{book.code}.zip"
        async with self._semaphore:
            await download_file(
                self._zip_url(book),
                zip_path,
                headers={"User-Agent": USER_AGENT},
            )
        await self._upload_pdfs(zip_path, book)
        await stage.completed()

    async def _upload_pdfs(self, zip_path: Path, book: Book) -> None:
        """Upload each PDF entry in one zip as a normalized raw GCS chapter."""
        headers = {"User-Agent": USER_AGENT}
        with zipfile.ZipFile(zip_path) as zf:
            pdf_entries = [n for n in zf.namelist() if n.lower().endswith(".pdf")]
            for entry in pdf_entries:
                stem = self._normalize_chapter_stem(entry, book.code)
                if await self._storage.raw_chapter_exists(book, stem):
                    continue
                data = zf.read(entry) or await get_bytes(self._chapter_pdf_url(entry), headers=headers)
                await self._storage.upload_raw_chapter(book, stem, data)

    # --- Naming ---

    def _zip_url(self, book: Book) -> str:
        """NCERT per-book zip URL built from the manifest code."""
        return BOOK_ZIP_URL_TEMPLATE.format(code=book.code)

    def _chapter_pdf_url(self, zip_entry: str) -> str:
        """Direct NCERT chapter PDF URL for one zip entry filename."""
        return CHAPTER_PDF_URL_TEMPLATE.format(entry=Path(zip_entry).name)

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
