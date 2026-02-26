"""
main.py — Entry point for the task-executor-agent.

Usage:
    python main.py [--orchestrator-url http://localhost:8000]
"""
from __future__ import annotations

import argparse
import asyncio
import logging

from orchestrator_client import OrchestratorClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Task Executor Agent")
    p.add_argument(
        "--orchestrator-url",
        default="http://localhost:8000",
        help="Base URL of the agent orchestrator (default: http://localhost:8000)",
    )
    return p.parse_args()


async def main() -> None:
    args = parse_args()
    logger.info("Starting task-executor-agent → %s", args.orchestrator_url)
    client = OrchestratorClient(orchestrator_url=args.orchestrator_url)
    await client.start()


if __name__ == "__main__":
    asyncio.run(main())
