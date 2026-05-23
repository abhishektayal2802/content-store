"""Content store cloud-job stage entry point."""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import UTC, datetime
from typing import get_args

from dotenv import load_dotenv

from infra.llm import OpenAIRuntime
from infra.platform.gcp import GcpIdentity
from infra.platform.secrets import SecretReader
from infra.platform.storage import GcsBucket
from infra.rag import VertexRagWriter

from .constants import (
    CLOUD_RUN_TASK_COUNT_ENV,
    CLOUD_RUN_TASK_INDEX_ENV,
    CONTENT_STORE_RUN_ID_ENV,
)
from .extractor import Extractor
from .publisher import Publisher
from .refresh_catalog import CatalogRefresher
from .run_state import StageRun
from .scraper import Scraper
from .storage import ContentStoreStorage
from .types import ContentStoreStage

load_dotenv()


async def main() -> None:
    """Run one explicit content-store stage."""
    stage = _stage_from_argv(sys.argv)
    gcp = GcpIdentity.from_env()
    bucket = GcsBucket(os.environ["CONTENT_STORE_GCS_BUCKET"], credentials=gcp.credentials)
    storage = ContentStoreStorage(bucket)
    task_index, task_count = _task_config()
    stage_run = StageRun(storage, _run_id(), stage, task_index, task_count)
    try:
        await _run_stage(stage, gcp, storage, stage_run.run_id, stage_run, task_index, task_count)
        await stage_run.succeed()
    except Exception:
        await stage_run.fail()
        raise
    finally:
        await bucket.close()


async def _run_stage(
    stage: ContentStoreStage,
    gcp: GcpIdentity,
    storage: ContentStoreStorage,
    run_id: str,
    stage_run: StageRun,
    task_index: int,
    task_count: int,
) -> None:
    """Dispatch one strict content-store stage."""
    match stage:
        case "refresh":
            await CatalogRefresher(storage, run_id).run(stage_run)
        case "scrape":
            await Scraper(storage, run_id).run(stage_run)
        case "extract":
            await _run_extract(gcp, storage, stage_run, task_index, task_count)
        case "stage":
            await Publisher(storage, run_id).stage(stage_run)
        case "publish":
            await _run_publish(gcp, storage, run_id, stage_run)


async def _run_extract(
    gcp: GcpIdentity,
    storage: ContentStoreStorage,
    stage_run: StageRun,
    task_index: int,
    task_count: int,
) -> None:
    """Run extraction with its OpenAI runtime lifetime scoped here."""
    secrets = SecretReader(gcp)
    runtime = OpenAIRuntime(secrets.get("OPENAI_API_KEY"))
    try:
        await Extractor(runtime, storage).run(stage_run, task_index, task_count)
    finally:
        await runtime.close()


async def _run_publish(
    gcp: GcpIdentity,
    storage: ContentStoreStorage,
    run_id: str,
    stage_run: StageRun,
) -> None:
    """Run publish with its Vertex writer lifetime scoped here."""
    rag = VertexRagWriter(identity=gcp)
    try:
        await Publisher(storage, run_id).publish(rag, stage_run)
    finally:
        await rag.close()


def _stage_from_argv(argv: list[str]) -> ContentStoreStage:
    """Parse the required stage argument."""
    stages = get_args(ContentStoreStage)
    if len(argv) != 2 or argv[1] not in stages:
        raise SystemExit(f"usage: python -m content_store.run {'|'.join(stages)}")
    return argv[1]


def _run_id() -> str:
    """Run id shared across manually chained jobs when CONTENT_STORE_RUN_ID is set."""
    return os.environ.get(CONTENT_STORE_RUN_ID_ENV) or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def _task_config() -> tuple[int, int]:
    """Cloud Run Job task index/count; local execution defaults to one task."""
    return (
        int(os.environ.get(CLOUD_RUN_TASK_INDEX_ENV, "0")),
        int(os.environ.get(CLOUD_RUN_TASK_COUNT_ENV, "1")),
    )


if __name__ == "__main__":
    asyncio.run(main())
