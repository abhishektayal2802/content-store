"""PDF page splitting helper shared by extract (stream) and publish (cache) phases."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pymupdf


async def split_pdf(path: Path) -> list[bytes]:
    """Split one PDF into per-page single-page PDF byte arrays (thread-offloaded)."""
    # pymupdf is pure CPU; run off the event loop so it doesn't block siblings.
    return await asyncio.to_thread(_split_sync, path)


def _split_sync(path: Path) -> list[bytes]:
    """Synchronously split one PDF into per-page byte arrays, preserving order."""
    doc = pymupdf.open(path)
    pages: list[bytes] = []
    for i in range(len(doc)):
        # Each page gets its own single-page PDF document so staged bytes are self-contained.
        single = pymupdf.open()
        single.insert_pdf(doc, from_page=i, to_page=i)
        pages.append(single.tobytes())
        single.close()
    doc.close()
    return pages
