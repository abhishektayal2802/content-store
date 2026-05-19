"""Content store pipeline entry point."""

from __future__ import annotations

import asyncio
import os

from dotenv import load_dotenv

from infra.llm import OpenAIRuntime
from infra.platform.gcp import GcpIdentity
from infra.platform.secrets import SecretReader
from infra.platform.storage import GcsBucket
from infra.rag import VertexRagWriter

from .pipeline import Pipeline

load_dotenv()


async def main() -> None:
    """Run the streaming content store pipeline."""
    # One identity, passed to every Google-facing client this process builds.
    gcp = GcpIdentity.from_env()
    secrets = SecretReader(gcp)
    # Run-scoped prefixes + bucket TTL own cleanup; no in-process staging cleanup.
    bucket = GcsBucket(os.environ["CONTENT_STORE_GCS_BUCKET"], credentials=gcp.credentials)

    runtime = OpenAIRuntime(secrets.get("OPENAI_API_KEY"))
    rag = VertexRagWriter(identity=gcp)
    try:
        await Pipeline(runtime, rag, bucket).run()
    finally:
        await rag.close()
        await bucket.close()
        await runtime.close()


if __name__ == "__main__":
    asyncio.run(main())
