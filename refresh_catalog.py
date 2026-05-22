"""Refresh the checked-in NCERT book catalog manifest.

Fetches ncert.nic.in/textbook.php, parses the JS book tables, resolves subjects
through the shared curriculum catalog, and writes catalog.json. This is the
only place that talks to the NCERT HTML catalog; the main pipeline reads
catalog.json.

Run from the monorepo root:

    python -m content_store.refresh_catalog
"""

from __future__ import annotations

import asyncio
import json

from infra.curriculum import resolve_ncert_subject, include_book
from infra.platform.http import create_client, get_text

from .constants import (
    BOOK_GROUP_PATTERN,
    BOOK_OPTION_PATTERN,
    CATALOG_PATH,
    CATALOG_URL,
    USER_AGENT,
)
from .types import Book


async def fetch_catalog_html() -> str:
    """Fetch the raw NCERT catalog HTML (with embedded JS book tables)."""
    client = create_client(headers={"User-Agent": USER_AGENT})
    try:
        return await get_text(CATALOG_URL, client=client)
    finally:
        await client.aclose()


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


def write_catalog(books: list[Book]) -> None:
    """Persist books to catalog.json in a stable, diff-friendly order."""
    sorted_books = sorted(books, key=lambda b: (b.grade, b.subject, b.code))
    payload = [b.model_dump() for b in sorted_books]
    CATALOG_PATH.write_text(json.dumps(payload, indent=2) + "\n")


async def main() -> None:
    """Fetch the NCERT catalog, filter, and write the manifest to disk."""
    html = await fetch_catalog_html()
    books = parse_catalog(html)
    write_catalog(books)
    print(f"Wrote {len(books)} books to {CATALOG_PATH}")


if __name__ == "__main__":
    asyncio.run(main())
