"""NCERT textbook PDF scraper."""

from __future__ import annotations

import asyncio
import re
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

from infra.http import create_client, get_bytes, get_text
from infra.text import slugify

from .constants import (
    ALLOWED_GRADES,
    ALLOWED_SUBJECTS,
    BOOK_GROUP_PATTERN,
    BOOK_OPTION_PATTERN,
    CATALOG_URL,
    INPUTS_ROOT,
    NCERT_BASE,
    SCRAPE_CONCURRENCY,
    SUFFIX_PATTERN,
    USER_AGENT,
)
from .reporter import ProgressReporter
from .types import Asset, Book


class Scraper:
    """Downloads NCERT chapter PDFs into the inputs directory."""

    def __init__(self) -> None:
        self._client = create_client(headers={"User-Agent": USER_AGENT})
        self._semaphore = asyncio.Semaphore(SCRAPE_CONCURRENCY)

    async def run(
        self,
        pdf_queue: asyncio.Queue[Optional[Path]],
        reporter: ProgressReporter,
    ) -> None:
        """Fetch catalog, filter books, download PDFs, push paths to queue."""
        catalog_html = await get_text(CATALOG_URL, client=self._client)
        books = [b for b in self._parse_catalog(catalog_html) if self._is_allowed(b)]

        if not books:
            await pdf_queue.put(None)
            return

        # Discover assets concurrently from per-book pages.
        asset_lists = await asyncio.gather(*(self._build_assets(b) for b in books))
        assets = [a for assets in asset_lists for a in assets]

        reporter.grow("scrape", len(assets))
        await self._download_all(assets, pdf_queue, reporter)
        await pdf_queue.put(None)

    async def close(self) -> None:
        """Release the HTTP client."""
        await self._client.aclose()

    # --- Catalog parsing ---

    def _parse_catalog(self, html: str) -> list[Book]:
        normalized = html.replace(r"\[", "[").replace(r"\]", "]")
        books: list[Book] = []
        for group in BOOK_GROUP_PATTERN.finditer(normalized):
            grade = int(group.group(1))
            # Standardize to slug form at the point of ingress
            subject = slugify(group.group(2).strip())
            body = group.group(3)
            books.extend(self._parse_book_group(grade, subject, body))
        return books

    def _parse_book_group(self, grade: int, subject: str, body: str) -> list[Book]:
        books: list[Book] = []
        for option in BOOK_OPTION_PATTERN.finditer(body):
            title = option.group(2).strip()
            code = option.group(3).strip()
            range_val = option.group(4).strip()
            book_url = urljoin(NCERT_BASE, f"textbook.php?{code}={range_val}")
            books.append(
                Book(
                    grade=grade,
                    subject=subject,
                    title=title,
                    code=code,
                    book_url=book_url,
                )
            )
        return books

    def _is_allowed(self, book: Book) -> bool:
        return (
            book.grade in ALLOWED_GRADES
            and book.subject in ALLOWED_SUBJECTS
            and len(book.code) > 1
            and book.code[1] == "e"
        )

    # --- Asset discovery ---

    async def _fetch_book_suffixes(self, book: Book) -> list[str]:
        """Fetch per-book page and extract all suffix values from JavaScript."""
        html = await get_text(book.book_url, client=self._client)
        return SUFFIX_PATTERN.findall(html)

    def _suffix_to_filename(self, suffix: str) -> Optional[str]:
        """Convert suffix to filename, or None if not a recognized asset type."""
        if re.fullmatch(r"\d{2}", suffix):
            return f"chapter-{suffix}.pdf"
        if suffix == "ps":
            return "prelims.pdf"
        if suffix == "an":
            return "answers.pdf"
        if m := re.fullmatch(r"a(\d+)", suffix):
            return f"appendix-{int(m.group(1)):02d}.pdf"
        if m := re.fullmatch(r"ax(\d*)", suffix):
            num = f"-{int(m.group(1)):02d}" if m.group(1) else ""
            return f"annexure{num}.pdf"
        if suffix in ("gl", "glo"):
            return "glossary.pdf"
        return None

    async def _build_assets(self, book: Book) -> list[Asset]:
        """Discover assets by parsing the per-book page."""
        suffixes = await self._fetch_book_suffixes(book)
        return [
            self._pdf_asset(book, filename, suffix)
            for suffix in suffixes
            if (filename := self._suffix_to_filename(suffix))
        ]

    def _pdf_asset(self, book: Book, filename: str, suffix: str) -> Asset:
        url = urljoin(NCERT_BASE, f"textbook/pdf/{book.code}{suffix}.pdf")
        return Asset(book=book, filename=filename, url=url)

    # --- Downloading ---

    async def _download_all(
        self,
        assets: list[Asset],
        pdf_queue: asyncio.Queue[Optional[Path]],
        reporter: ProgressReporter,
    ) -> None:
        """Download all assets concurrently, pushing paths to queue."""
        await asyncio.gather(
            *(self._download_one(a, pdf_queue, reporter) for a in assets)
        )

    async def _download_one(
        self,
        asset: Asset,
        pdf_queue: asyncio.Queue[Optional[Path]],
        reporter: ProgressReporter,
    ) -> None:
        """Download one PDF, push path to queue if successful."""
        path = self._output_path(asset)
        path.parent.mkdir(parents=True, exist_ok=True)

        try:
            async with self._semaphore:
                content = await get_bytes(asset.url, client=self._client)
            await asyncio.to_thread(path.write_bytes, content)
            await pdf_queue.put(path)
            reporter.advance("scrape")
        except Exception as e:
            reporter.record_error("scrape", asset.url, e)

    def _output_path(self, asset: Asset) -> Path:
        return (
            INPUTS_ROOT
            / str(asset.book.grade)
            / asset.book.subject
            / slugify(asset.book.title)
            / asset.filename
        )
