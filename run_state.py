"""Centralized stage manifests and structured errors."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from .constants import TELEMETRY_FLUSH_UNITS
from .storage import ContentStoreStorage
from .types import ContentStoreStage, RunError, StageManifest


class StageRun:
    """One stage's GCS manifest writer."""

    def __init__(
        self,
        storage: ContentStoreStorage,
        run_id: str,
        stage: ContentStoreStage,
        task_index: int,
        task_count: int,
    ) -> None:
        self._storage = storage
        self._manifest = StageManifest(
            run_id=run_id,
            stage=stage,
            status="running",
            total=0,
            completed=0,
            skipped=0,
            failed=0,
            task_index=task_index,
            task_count=task_count,
            started_at=_now(),
            updated_at=_now(),
        )
        self._lock = asyncio.Lock()

    @property
    def run_id(self) -> str:
        """Run identifier shared across stage outputs."""
        return self._manifest.run_id

    async def start(self, total: int) -> None:
        """Write the initial stage manifest."""
        self._manifest.total = total
        await self._flush()

    async def planned(self, count: int) -> None:
        """Record newly discovered planned units."""
        self._manifest.total += count

    async def completed(self, count: int = 1) -> None:
        """Record completed units, flushing periodically."""
        self._manifest.completed += count
        await self._flush_periodically()

    async def skipped(self, count: int = 1) -> None:
        """Record skipped units, flushing periodically."""
        self._manifest.skipped += count
        await self._flush_periodically()

    async def record_error(self, context: str, exc: BaseException) -> None:
        """Persist one structured error record."""
        async with self._lock:
            self._manifest.failed += 1
            error = RunError(
                run_id=self.run_id,
                stage=self._manifest.stage,
                context=context,
                error_type=type(exc).__name__,
                message=str(exc) or type(exc).__name__,
                task_index=self._manifest.task_index,
                timestamp=_now(),
            )
            await self._storage.append_run_error(error)
            await self._flush()

    async def succeed(self) -> None:
        """Mark this stage as successful."""
        self._manifest.status = "succeeded"
        await self._flush()

    async def fail(self) -> None:
        """Mark this stage as failed and persist the failure message."""
        self._manifest.status = "failed"
        await self._flush()

    async def _flush_periodically(self) -> None:
        """Flush every configured number of completed/skipped units."""
        done = self._manifest.completed + self._manifest.skipped + self._manifest.failed
        if done and done % TELEMETRY_FLUSH_UNITS == 0:
            await self._flush()

    async def _flush(self) -> None:
        """Write the current stage manifest to GCS."""
        self._manifest.updated_at = _now()
        await self._storage.write_stage_manifest(self._manifest)


def _now() -> str:
    """Current UTC timestamp for manifests."""
    return datetime.now(UTC).isoformat()
