"""Terminal stage: trigger per-corpus import LROs, then attach metadata.

After the stager has finished uploading all bytes to GCS, this module:
1. Fires one `import_rag_files` LRO per corpus (3 total), in parallel.
   Each LRO pulls every object under `staging/<corpus>/*` via a single
   wildcard URI -- no per-file URI list, no 25-URI server cap.
2. For each completed import, enumerates the corpus's rag files, matches
   each back to its staged manifest entry by GCS URI, and attaches per-file
   metadata via `batch_create_rag_metadata` in parallel.

Progress model: one `import` task per corpus, advanced on LRO completion.
"""

from __future__ import annotations

import asyncio

from infra.rag import CorpusKind, VertexRagClient

from .reporter import ProgressReporter
from .types import CorpusManifest, StagedFile


class Importer:
    """Drives import LROs and post-import metadata attachment."""

    def __init__(self, rag: VertexRagClient) -> None:
        """Bind to the shared RAG client."""
        self._rag = rag

    async def run(
        self,
        manifest: CorpusManifest,
        reporter: ProgressReporter,
    ) -> None:
        """Import every non-empty corpus in parallel, then attach metadata."""
        pending = [(c, entries) for c, entries in manifest.items() if entries]
        reporter.grow("import", len(pending))
        await asyncio.gather(*[
            self._import_corpus(c, entries, reporter) for c, entries in pending
        ])

    async def _import_corpus(
        self,
        corpus: CorpusKind,
        entries: list[StagedFile],
        reporter: ProgressReporter,
    ) -> None:
        """Import one corpus; attach each file's metadata in parallel."""
        try:
            failed = await self._rag.import_corpus(corpus)
            if failed:
                reporter.record_error(
                    "import", corpus,
                    RuntimeError(f"{failed} files failed to import"),
                )
            # Match imported RagFiles back to staged entries by GCS URI so we
            # can attach per-file metadata. One list RPC feeds N attach RPCs.
            by_uri = await self._rag.list_files_by_gcs_uri(corpus)
            await asyncio.gather(*[
                self._rag.attach_metadata(by_uri[e.gcs_uri], e.metadata)
                for e in entries if e.gcs_uri in by_uri
            ])
            reporter.advance("import")
        except Exception as e:
            reporter.record_error("import", corpus, e)
