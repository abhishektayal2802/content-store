"""CLI progress bars + error summary for the pipeline."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

from .constants import STAGE_LABELS
from .types import Stage


class StageReporter:
    """Per-stage view bound to a ProgressReporter: no stage string at call sites."""

    def __init__(self, parent: ProgressReporter, stage: Stage) -> None:
        self._parent = parent
        self._stage = stage

    def grow(self, delta: int) -> None:
        """Increase this stage's total by delta (bar appears on first call)."""
        self._parent._grow(self._stage, delta)

    def advance(self) -> None:
        """Mark one unit of this stage's work complete."""
        self._parent._advance(self._stage)

    def record_error(self, context: str, exc: BaseException) -> None:
        """Record a non-fatal error for this stage."""
        self._parent._record_error(self._stage, context, exc)


class ProgressReporter:
    """Owns the rich Progress view + error buffer; exposes per-stage bound views."""

    def __init__(self) -> None:
        self._console = Console()
        self._progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(),
            console=self._console,
        )
        self._tasks: dict[Stage, TaskID] = {}
        self._errors: list[tuple[Stage, str, str]] = []
        # Workers receive one of these views; never the parent.
        self.scrape = StageReporter(self, "scrape")
        self.extract = StageReporter(self, "extract")
        self.stage = StageReporter(self, "stage")
        self.importer = StageReporter(self, "import")

    @contextmanager
    def live(self) -> Iterator[ProgressReporter]:
        """Start/stop the progress display; print error summary on exit."""
        try:
            with self._progress:
                yield self
        finally:
            self._print_summary()

    # Package-private: only StageReporter calls these.
    def _grow(self, stage: Stage, delta: int) -> None:
        # Lazy bar creation: a stage only shows up once it has work.
        task_id = self._tasks.get(stage) or self._add_task(stage)
        current = self._progress.tasks[task_id].total or 0
        self._progress.update(task_id, total=current + delta)

    def _advance(self, stage: Stage) -> None:
        self._progress.advance(self._tasks[stage])

    def _record_error(self, stage: Stage, context: str, exc: BaseException) -> None:
        # str(exc) can be empty (e.g. RuntimeError()); fall back to class name.
        msg = str(exc) or type(exc).__name__
        self._errors.append((stage, context, msg))

    def _add_task(self, stage: Stage) -> TaskID:
        task_id = self._progress.add_task(STAGE_LABELS[stage], total=None)
        self._tasks[stage] = task_id
        return task_id

    def _print_summary(self) -> None:
        """Render the error table (nothing shown on a clean run)."""
        if not self._errors:
            return
        table = Table(title=f"{len(self._errors)} Errors", show_lines=True)
        table.add_column("Stage", style="bold")
        table.add_column("Context")
        table.add_column("Error", style="red")
        for stage, context, msg in self._errors[:20]:
            table.add_row(stage, context, msg[:80])
        self._console.print()
        self._console.print(table)
        if len(self._errors) > 20:
            self._console.print(f"... and {len(self._errors) - 20} more errors")
