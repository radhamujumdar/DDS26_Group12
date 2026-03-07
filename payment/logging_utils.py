import json
import logging
from datetime import datetime, timezone


def log_event(logger: logging.Logger, event: str, level: str = "info", **fields):
    payload = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": event,
        **fields,
    }
    log_level = getattr(logging, level.upper(), logging.INFO)
    logger.log(log_level, json.dumps(payload, separators=(",", ":"), default=str))
