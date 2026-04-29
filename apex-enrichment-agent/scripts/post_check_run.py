"""Run the J+1 post-check on all drafts older than 24h."""
from __future__ import annotations

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv

load_dotenv()

from lib.post_check import run_post_check  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format='{"ts":"%(asctime)s","lvl":"%(levelname)s","msg":"%(message)s"}',
)
logger = logging.getLogger("post_check")


if __name__ == "__main__":
    stats = run_post_check()
    logger.info(f'{{"post_check":"done","stats":{stats}}}')
