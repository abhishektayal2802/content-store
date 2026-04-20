"""Content store pipeline entry point."""

from __future__ import annotations

import asyncio
import os

from dotenv import load_dotenv

from infra.gcp import GcpIdentity
from infra.llm import GeminiRuntime
from infra.rag import VertexRagWriter
from infra.secrets import SecretReader

from .pipeline import Pipeline

load_dotenv()


async def main() -> None:
    """Run the streaming content store pipeline."""
    # One identity, passed to every Google-facing client this process builds.
    gcp = GcpIdentity.from_env()
    bucket = os.environ["CONTENT_STORE_GCS_BUCKET"]
    api_key = SecretReader(gcp).get("GEMINI_API_KEY")

    runtime = GeminiRuntime(api_key)
    rag = VertexRagWriter(identity=gcp, bucket=bucket)
    try:
        await Pipeline(runtime, rag).run()
    finally:
        await rag.close()
        await runtime.close()


if __name__ == "__main__":
    asyncio.run(main())
