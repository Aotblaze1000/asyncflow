"""Container startup helper: prepare storage and the schema, then hand over.

The API creates tables during its lifespan, but workers and the beat scheduler
start without that lifespan, so every container runs this first.
"""

from __future__ import annotations

import logging

from app.core.config import settings
from app.database.connection import init_db_with_retry

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")


def main() -> None:
    settings.ensure_storage_dir()
    init_db_with_retry()
    logging.getLogger(__name__).info("Database schema is ready")


if __name__ == "__main__":
    main()
