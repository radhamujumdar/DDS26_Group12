"""Shared logging and timing helpers for local observability."""

from __future__ import annotations

import logging
import os
import time


_TRUE_VALUES = {"1", "true", "yes", "on"}


def trace_logging_enabled() -> bool:
    value = os.getenv("DDS_TRACE_LOGGING", "0")
    return value.strip().lower() in _TRUE_VALUES


def configure_logging(service_name: str) -> None:
    level_name = os.getenv("DDS_LOG_LEVEL", "INFO").strip().upper() or "INFO"
    level = getattr(logging, level_name, logging.INFO)
    root_logger = logging.getLogger()

    if not root_logger.handlers:
        logging.basicConfig(
            level=level,
            format=(
                "%(asctime)s %(levelname)s "
                "[pid=%(process)d] [%(name)s] %(message)s"
            ),
        )
    else:
        root_logger.setLevel(level)
        for handler in root_logger.handlers:
            handler.setLevel(level)

    logging.getLogger(service_name).setLevel(level)


def elapsed_ms(start_time: float) -> float:
    return (time.perf_counter() - start_time) * 1000


def redis_stream_message_age_ms(message_id: str) -> int | None:
    timestamp_text, _, _ = message_id.partition("-")
    if not timestamp_text:
        return None
    try:
        enqueued_at_ms = int(timestamp_text)
    except ValueError:
        return None
    return max(0, int(time.time() * 1000) - enqueued_at_ms)
