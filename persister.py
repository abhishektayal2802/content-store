"""Upload extracted content to Gemini File Search stores."""

from __future__ import annotations

import asyncio
from typing import Optional

from infra.content import ContentMarkdownRenderer
from infra.llm import GeminiFilesClient, GeminiRuntime

from .prompts import STORE_FIELDS, STORE_KINDS
from .queues import iter_queue
from .reporter import ProgressReporter
from .types import Document, ExtractedPage, StoreKind


class Persister:
    """Uploads pages and markdown to File Search stores."""

    def __init__(self, runtime: GeminiRuntime) -> None:
        """Initialize persister with shared Gemini runtime."""
        self._files = GeminiFilesClient(runtime)
        self._renderer = ContentMarkdownRenderer()

    async def setup(self) -> tuple[dict[StoreKind, str], set[str]]:
        """Ensure stores exist, return stores and set of completed page_keys."""
        stores = await self._ensure_stores()
        done = await self._list_completed_pages(stores["pages"])
        return stores, done

    async def run(
        self,
        stores: dict[StoreKind, str],
        page_queue: asyncio.Queue[Optional[ExtractedPage]],
        reporter: ProgressReporter,
    ) -> None:
        """Consume pages from queue, build documents, upload."""
        async for page in iter_queue(page_queue):
            docs = self._build_documents(page)
            reporter.grow("persist", len(docs))
            await self._upload_all(stores, docs, reporter)

    # --- Store management ---

    async def _ensure_stores(self) -> dict[StoreKind, str]:
        """Create or find each store, returning kind -> store name."""
        return {kind: await self._files.ensure_store(kind) for kind in STORE_KINDS}

    async def _list_completed_pages(self, pages_store: str) -> set[str]:
        """List page_keys that are already in the pages store."""
        docs = await self._files.list_documents_full(pages_store)
        return {
            doc.display_name.removeprefix("pages__")
            for doc in docs
            if doc.display_name
        }

    # --- Document building ---

    def _build_documents(self, page: ExtractedPage) -> list[Document]:
        """Build the page PDF document plus one markdown doc per extraction field."""
        docs: list[Document] = [
            Document(
                store="pages",
                name=f"{page.meta.display_name('pages')}.pdf",
                content=page.pdf_bytes,
                mime="application/pdf",
                meta=page.meta,
            )
        ]

        for field in STORE_FIELDS:
            for i, item in enumerate(getattr(page.extraction, field), 1):
                docs.append(Document(
                    store=field,
                    name=f"{page.meta.display_name(field)}__item-{i:03d}.md",
                    content=self._renderer.render(item).encode("utf-8"),
                    mime="text/markdown",
                    meta=page.meta,
                ))

        return docs

    # --- Upload ---

    async def _upload_all(
        self,
        stores: dict[StoreKind, str],
        docs: list[Document],
        reporter: ProgressReporter,
    ) -> None:
        """Upload all documents for one page concurrently."""

        async def _upload(doc: Document) -> None:
            """Upload one document and advance progress."""
            try:
                await self._files.store_bytes(
                    store_name=stores[doc.store],
                    data=doc.content,
                    config=doc.upload_config(),
                )
            except Exception as e:
                reporter.record_error("persist", doc.name, e)
            reporter.advance("persist")

        await asyncio.gather(*(_upload(d) for d in docs))
