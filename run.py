"""Content store pipeline entry point."""

from __future__ import annotations

import asyncio
import os

from dotenv import load_dotenv

from infra.llm import GeminiRuntime
from infra.secrets import SecretReader

from .pipeline import Pipeline

load_dotenv()


async def main() -> None:
    """Run the streaming content store pipeline."""
    project = os.environ["GOOGLE_CLOUD_PROJECT"]
    location = os.environ["GOOGLE_CLOUD_LOCATION"]
    api_key = SecretReader(project, location).get("GEMINI_API_KEY")

    runtime = GeminiRuntime(api_key)
    try:
        await Pipeline(runtime).run()
    finally:
        await runtime.close()


if __name__ == "__main__":
    asyncio.run(main())
