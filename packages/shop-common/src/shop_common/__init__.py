"""Shared project contracts and infrastructure for the shopping-cart services."""

from .config import GatewaySettings, RedisSettings, required_env
from .errors import DatabaseError, UpstreamServiceError
from .http import MessageResponse
from .redis import (
    activity_dedupe_key,
    close_redis_client,
    create_redis_client,
    decode_dedupe_record,
    encode_error_record,
    encode_success_record,
)

__all__ = [
    "DatabaseError",
    "GatewaySettings",
    "MessageResponse",
    "RedisSettings",
    "UpstreamServiceError",
    "activity_dedupe_key",
    "close_redis_client",
    "create_redis_client",
    "decode_dedupe_record",
    "encode_error_record",
    "encode_success_record",
    "required_env",
]
