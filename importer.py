"""Terminal stage: one import LRO per corpus, then attach per-file metadata."""

from __future__ import annotations

import asyncio

from infra.rag import CorpusKind, VertexRagClient

from .reporter import StageReporter
from .types import CorpusManifest, StagedFile


class Importer:
    """Drives import LROs and post-import metadata attachment."""

    def __init__(self, rag: VertexRagClient) -> None:
        self._rag = rag

    async def run(
        self,
        manifest: CorpusManifest,
        reporter: StageReporter,
    ) -> None:
        """Import every non-empty corpus in parallel, then attach metadata."""
        pending = [(c, entries) for c, entries in manifest.items() if entries]
        reporter.grow(len(pending))
        await asyncio.gather(*[
            self._import_corpus(c, entries, reporter) for c, entries in pending
        ])

    async def _import_corpus(
        self,
        corpus: CorpusKind,
        entries: list[StagedFile],
        reporter: StageReporter,
    ) -> None:
        """Import one corpus; attach each file's metadata in parallel."""
        try:
            failed = await self._rag.import_corpus(corpus)
            if failed:
                reporter.record_error(
                    corpus, RuntimeError(f"{failed} files failed to import"),
                )
            # Match imported RagFiles back to staged entries by GCS URI.
            by_uri = await self._rag.list_files_by_gcs_uri(corpus)
            await asyncio.gather(*[
                self._rag.attach_metadata(by_uri[e.gcs_uri], e.metadata)
                for e in entries if e.gcs_uri in by_uri
            ])
            reporter.advance()
        except Exception as e:
            reporter.record_error(corpus, e)
