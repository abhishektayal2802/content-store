"""Refresh the NCERT book catalog manifest.

Fetches ncert.nic.in/textbook.php, parses the JS book tables, resolves subjects
through the shared curriculum catalog, and keeps only books whose NCERT zip
exists. This is the only code that talks to the NCERT HTML catalog.

Run from the monorepo root:

    python -m content_store.refresh_catalog
"""

from __future__ import annotations

import asyncio
import json

from httpx import AsyncClient

from infra.curriculum import resolve_ncert_subject, include_book
from infra.platform.http import create_client, get_text
from infra.platform.retry import retry

from .constants import (
    BOOK_GROUP_PATTERN,
    BOOK_OPTION_PATTERN,
    BOOK_ZIP_URL_TEMPLATE,
    CATALOG_PATH,
    CATALOG_URL,
    USER_AGENT,
    ZIP_CONCURRENCY,
)
from .run_state import StageRun
from .storage import ContentStoreStorage
from .types import Book


class CatalogRefresher:
    """Build and persist the validated run catalog before scrape."""

    def __init__(self, storage: ContentStoreStorage, run_id: str) -> None:
        self._storage = storage
        self._run_id = run_id

    async def run(self, stage: StageRun) -> None:
        """Refresh NCERT catalog state and write it as a run artifact."""
        candidates = await fetch_catalog()
        await stage.start(len(candidates))
        books = await validate_catalog(candidates)
        await stage.completed(len(books))
        await stage.skipped(len(candidates) - len(books))
        await self._storage.write_catalog(self._run_id, books)


async def fetch_catalog_html() -> str:
    """Fetch the raw NCERT catalog HTML (with embedded JS book tables)."""
    async with create_client(headers={"User-Agent": USER_AGENT}) as client:
        return await get_text(CATALOG_URL, client=client)


def parse_catalog(html: str) -> list[Book]:
    """Parse the NCERT catalog HTML into a flat list of Book entries."""
    # NCERT escapes brackets in the embedded JS; unescape before regexing.
    normalized = html.replace(r"\[", "[").replace(r"\]", "]")
    books: list[Book] = []
    for group in BOOK_GROUP_PATTERN.finditer(normalized):
        grade = int(group.group(1))
        subject = resolve_ncert_subject(grade, group.group(2).strip())
        if subject is None:
            continue
        for match in BOOK_OPTION_PATTERN.finditer(group.group(3)):
            code = match.group(3).strip()
            if not include_book(subject, code):
                continue
            books.append(Book(
                grade=grade,
                subject=subject.value,
                title=match.group(2).strip(),
                code=code,
            ))
    return books


async def fetch_catalog() -> list[Book]:
    """Fetch and parse the live NCERT catalog."""
    return parse_catalog(await fetch_catalog_html())


async def validate_catalog(books: list[Book]) -> list[Book]:
    """Keep only books whose NCERT zip endpoint exists."""
    semaphore = asyncio.Semaphore(ZIP_CONCURRENCY)
    async with create_client(headers={"User-Agent": USER_AGENT}) as client:
        exists = await asyncio.gather(*[
            _zip_exists(book, client, semaphore)
            for book in books
        ])
    return [book for book, zip_exists in zip(books, exists) if zip_exists]


@retry()
async def _zip_exists(book: Book, client: AsyncClient, semaphore: asyncio.Semaphore) -> bool:
    """True when NCERT serves the book zip; 404 means stale catalog entry."""
    async with semaphore:
        resp = await client.head(_zip_url(book), follow_redirects=True)
    if resp.status_code == 404:
        return False
    resp.raise_for_status()
    return True


def _zip_url(book: Book) -> str:
    """NCERT per-book zip URL built from the manifest code."""
    return BOOK_ZIP_URL_TEMPLATE.format(code=book.code)


def write_catalog(books: list[Book]) -> None:
    """Persist books to catalog.json in a stable, diff-friendly order."""
    sorted_books = sorted(books, key=lambda b: (b.grade, b.subject, b.code))
    payload = [b.model_dump() for b in sorted_books]
    CATALOG_PATH.write_text(json.dumps(payload, indent=2) + "\n")


async def main() -> None:
    """Fetch the NCERT catalog, filter, and write the manifest to disk."""
    books = await validate_catalog(await fetch_catalog())
    write_catalog(books)
    print(f"Wrote {len(books)} books to {CATALOG_PATH}")


if __name__ == "__main__":
    asyncio.run(main())
