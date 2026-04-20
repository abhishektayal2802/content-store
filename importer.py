"""Terminal stage: bin-pack staged files into LROs, import, attach metadata."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass, field

from infra.constants import Const
from infra.rag import CorpusKind, VertexRagWriter

from .reporter import StageReporter
from .types import CorpusManifest, StagedFile


class Importer:
    """Bin-packs corpora into LRO-safe batches, imports in parallel, attaches metadata."""

    def __init__(self, rag: VertexRagWriter) -> None:
        self._rag = rag

    async def run(
        self,
        manifest: CorpusManifest,
        reporter: StageReporter,
    ) -> None:
        """Fan out LROs across all corpora; attach metadata per corpus afterwards."""
        plan = {c: _pack(_shard_counts(entries))
                for c, entries in manifest.items() if entries}
        reporter.grow(sum(len(batches) for batches in plan.values()))
        await asyncio.gather(*[
            self._run_corpus(c, batches, manifest[c], reporter)
            for c, batches in plan.items()
        ])

    async def _run_corpus(
        self,
        corpus: CorpusKind,
        batches: list[list[str]],
        entries: list[StagedFile],
        reporter: StageReporter,
    ) -> None:
        """Run a corpus's LROs in parallel; on success, attach per-file metadata."""
        try:
            failed_counts = await asyncio.gather(*[
                self._run_lro(corpus, b, reporter) for b in batches
            ])
            failed = sum(failed_counts)
            if failed:
                reporter.record_error(
                    corpus, RuntimeError(f"{failed} files failed to import"),
                )
            # Join imported RagFiles back to staged entries by GCS URI.
            by_uri = await self._rag.list_files_by_gcs_uri(corpus)
            await asyncio.gather(*[
                self._rag.attach_metadata(by_uri[e.gcs_uri], e.metadata)
                for e in entries if e.gcs_uri in by_uri
            ])
        except Exception as e:
            reporter.record_error(corpus, e)

    async def _run_lro(
        self, corpus: CorpusKind, prefix_uris: list[str], reporter: StageReporter,
    ) -> int:
        """Run one import LRO; advance the progress bar on completion."""
        failed = await self._rag.import_prefixes(corpus, prefix_uris)
        reporter.advance()
        return failed


@dataclass
class _Batch:
    """Accumulator for First-Fit-Decreasing packing."""
    uris: list[str] = field(default_factory=list)
    files: int = 0


def _shard_counts(entries: list[StagedFile]) -> dict[str, int]:
    """Group entries by GCS directory; return {prefix_uri: file_count}."""
    counts: dict[str, int] = defaultdict(int)
    for e in entries:
        counts[e.gcs_uri.rsplit("/", 1)[0]] += 1
    return counts


def _pack(shard_counts: dict[str, int]) -> list[list[str]]:
    """First-Fit-Decreasing pack into LRO batches honoring both Vertex caps."""
    max_uris = Const.Rag.MAX_URIS_PER_REQUEST
    max_files = Const.Rag.MAX_FILES_PER_LRO
    batches: list[_Batch] = []
    for prefix, count in sorted(shard_counts.items(), key=lambda kv: -kv[1]):
        fitted = next(
            (b for b in batches
             if len(b.uris) < max_uris and b.files + count <= max_files),
            None,
        )
        if fitted is None:
            fitted = _Batch()
            batches.append(fitted)
        fitted.uris.append(prefix)
        fitted.files += count
    return [b.uris for b in batches]
