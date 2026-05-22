"""PDF page splitting helper shared by extract and publish stages."""

from __future__ import annotations

import asyncio

import pymupdf


async def split_pdf(data: bytes) -> list[bytes]:
    """Split one PDF into per-page single-page PDF byte arrays (thread-offloaded)."""
    return await asyncio.to_thread(_split_sync, data)


def _split_sync(data: bytes) -> list[bytes]:
    """Synchronously split one PDF into per-page byte arrays, preserving order."""
    doc = pymupdf.open(stream=data, filetype="pdf")
    pages: list[bytes] = []
    for i in range(len(doc)):
        # Each page gets its own single-page PDF document so staged bytes are self-contained.
        single = pymupdf.open()
        single.insert_pdf(doc, from_page=i, to_page=i)
        pages.append(single.tobytes())
        single.close()
    doc.close()
    return pages
