"""Durable local cache of LLM-extracted pages; the extract-stage resume signal.

The cache is the single source of truth for "this page has been extracted".
Publish always rebuilds remote state from this closed local cache, so there is
no remote sentinel / cross-run in-memory manifest to keep consistent.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterator

from infra.content import PageMeta

from .constants import EXTRACTED_ROOT
from .types import CachedPage


class PageCache:
    """Filesystem-backed extracted-page cache; mirrors the inputs/ tree one-to-one."""

    def __init__(self, root: Path = EXTRACTED_ROOT) -> None:
        self._root = root

    def exists(self, meta: PageMeta) -> bool:
        """True if this page's extraction JSON is already on disk."""
        return self._path(meta).exists()

    def write(self, page: CachedPage) -> None:
        """Atomically write one page's extraction; readers never see partial files."""
        path = self._path(page.meta)
        path.parent.mkdir(parents=True, exist_ok=True)
        # Temp-file + rename keeps writes atomic on local filesystems.
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(page.model_dump_json())
        os.replace(tmp, path)

    def iter_all(self) -> Iterator[CachedPage]:
        """Iterate every cached page on disk in a stable order (sorted by path)."""
        # Sorting by path naturally groups by chapter, which is what publish wants.
        for path in sorted(self._root.rglob("*.json")):
            yield CachedPage.model_validate_json(path.read_text())

    def _path(self, meta: PageMeta) -> Path:
        """Cache path: `<EXTRACTED_ROOT>/<grade>/<subject>/<book>/<chapter>__page-<NNN>.json`."""
        # Zero-pad page to keep lexical order == numerical order within a chapter.
        return (
            self._root
            / str(meta.grade) / meta.subject / meta.book
            / f"{meta.chapter}__page-{meta.page:03d}.json"
        )
