"""Upload extracted content to Gemini File Search stores."""

from __future__ import annotations

import asyncio
from typing import Optional

from google.genai import types

from infra.content import (
    ContentMarkdownRenderer,
    Document,
    ExtractedPage,
    PendingIndex,
    QUESTION_STORES,
    StoreKind,
)
from infra.llm import GeminiFilesClient, GeminiRuntime

from .prompts import STORE_FIELDS, STORE_KINDS
from .queues import iter_queue
from .reporter import ProgressReporter


def _upload_config(doc: Document) -> types.UploadToFileSearchStoreConfig:
    """Build the Google SDK upload config for a document."""
    metadata = [
        types.CustomMetadata(key="grade", numeric_value=doc.meta.grade),
        types.CustomMetadata(key="subject", string_value=doc.meta.subject),
        types.CustomMetadata(key="book", string_value=doc.meta.book),
        types.CustomMetadata(key="chapter", string_value=doc.meta.chapter),
        types.CustomMetadata(key="page", numeric_value=doc.meta.page),
    ]
    if doc.difficulty:
        metadata.append(types.CustomMetadata(key="difficulty", string_value=doc.difficulty))
    return types.UploadToFileSearchStoreConfig(
        display_name=doc.name,
        mime_type=doc.mime,
        custom_metadata=metadata,
    )


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
        op_queue: asyncio.Queue[Optional[PendingIndex]],
        reporter: ProgressReporter,
    ) -> None:
        """Consume pages from queue, build documents, upload."""
        tasks = []
        async for page in iter_queue(page_queue):
            docs = self._build_documents(page)
            reporter.grow("persist", len(docs))
            tasks.append(asyncio.create_task(
                self._upload_all(stores, docs, op_queue, reporter)
            ))

        await asyncio.gather(*tasks)
        await op_queue.put(None)

    # --- Store management ---

    async def _ensure_stores(self) -> dict[StoreKind, str]:
        """Create or find each store, returning kind -> store name."""
        return {kind: await self._files.ensure_store(kind) for kind in STORE_KINDS}

    async def _list_completed_pages(self, pages_store: str) -> set[str]:
        """List page_keys that are already in the pages store."""
        docs = await self._files.list_documents_full(pages_store)
        return {
            doc.display_name.removeprefix("pages__").removesuffix(".pdf")
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
            is_question = field in QUESTION_STORES
            for i, item in enumerate(getattr(page.extraction, field), 1):
                docs.append(Document(
                    store=field,
                    name=f"{page.meta.display_name(field)}__item-{i:03d}.md",
                    content=self._renderer.render(item).encode("utf-8"),
                    mime="text/markdown",
                    meta=page.meta,
                    difficulty=item.difficulty if is_question else None,
                ))

        return docs

    # --- Upload ---

    async def _upload_all(
        self,
        stores: dict[StoreKind, str],
        docs: list[Document],
        op_queue: asyncio.Queue[Optional[PendingIndex]],
        reporter: ProgressReporter,
    ) -> None:
        """Upload all documents for one page concurrently."""

        async def _upload(doc: Document) -> None:
            """Upload one document and advance progress on success."""
            try:
                operation = await self._files.store_bytes(
                    store_name=stores[doc.store],
                    data=doc.content,
                    config=_upload_config(doc),
                )                
                await op_queue.put(PendingIndex(name=doc.name, operation=operation))
                reporter.advance("persist")
            except Exception as e:
                reporter.record_error("persist", doc.name, e)

        await asyncio.gather(*(_upload(d) for d in docs))
