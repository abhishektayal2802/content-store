"""Refresh the checked-in NCERT book catalog manifest.

Fetches ncert.nic.in/textbook.php, parses the JS book tables, keeps books that
pass our grade/subject allowlist, and writes catalog.json. This is the only
place that talks to the NCERT HTML catalog; the main pipeline reads catalog.json.

Run from the monorepo root:

    python -m content_store.refresh_catalog
"""

from __future__ import annotations

import asyncio
import json

from infra.http import create_client, get_text
from infra.text import slugify

from .constants import (
    ALLOWED_GRADES,
    ALLOWED_SUBJECTS,
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
        subject = slugify(group.group(2).strip())
        body = group.group(3)
        books.extend(_parse_book_group(grade, subject, body))
    return books


def _parse_book_group(grade: int, subject: str, body: str) -> list[Book]:
    """Parse all book options within one (grade, subject) group."""
    return [
        Book(
            grade=grade,
            subject=subject,
            title=m.group(2).strip(),
            code=m.group(3).strip(),
        )
        for m in BOOK_OPTION_PATTERN.finditer(body)
    ]


def is_allowed(book: Book) -> bool:
    """Keep only English-edition books in supported grades/subjects."""
    # NCERT codes follow a "<lang>e<id>" convention; require English edition.
    return (
        book.grade in ALLOWED_GRADES
        and book.subject in ALLOWED_SUBJECTS
        and len(book.code) > 1
        and book.code[1] == "e"
    )


def write_catalog(books: list[Book]) -> None:
    """Persist books to catalog.json in a stable, diff-friendly order."""
    sorted_books = sorted(books, key=lambda b: (b.grade, b.subject, b.code))
    payload = [b.model_dump() for b in sorted_books]
    CATALOG_PATH.write_text(json.dumps(payload, indent=2) + "\n")


async def main() -> None:
    """Fetch the NCERT catalog, filter, and write the manifest to disk."""
    html = await fetch_catalog_html()
    books = [b for b in parse_catalog(html) if is_allowed(b)]
    write_catalog(books)
    print(f"Wrote {len(books)} books to {CATALOG_PATH}")


if __name__ == "__main__":
    asyncio.run(main())
