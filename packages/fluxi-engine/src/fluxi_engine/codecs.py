"""Binary payload helpers for the Fluxi engine."""

from __future__ import annotations

import base64
from typing import Any

from msgspec import msgpack
import msgpack as raw_msgpack


def packb(payload: Any) -> bytes:
    """Encode a Python value as msgpack bytes."""

    return msgpack.encode(payload)


def unpackb(payload: bytes | bytearray | memoryview | None) -> Any:
    """Decode msgpack bytes into a Python value."""

    if payload is None:
        return None
    buffer = bytes(payload)
    try:
        return msgpack.decode(buffer)
    except UnicodeDecodeError:
        return _normalize_lua_msgpack_value(raw_msgpack.unpackb(buffer, raw=True))


def to_base64(payload: bytes | None) -> str | None:
    """Encode raw bytes for JSON transport."""

    if payload is None:
        return None
    return base64.b64encode(payload).decode("ascii")


def from_base64(payload: str | None) -> bytes | None:
    """Decode base64 bytes received over the HTTP API."""

    if payload is None:
        return None
    return base64.b64decode(payload.encode("ascii"))


def _normalize_lua_msgpack_value(value: Any, *, field_name: str | None = None) -> Any:
    if isinstance(value, dict):
        normalized: dict[str, Any] = {}
        for raw_key, raw_value in value.items():
            key = (
                raw_key.decode("utf-8")
                if isinstance(raw_key, bytes)
                else str(raw_key)
            )
            normalized[key] = _normalize_lua_msgpack_value(raw_value, field_name=key)
        return normalized
    if isinstance(value, list):
        return [_normalize_lua_msgpack_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_normalize_lua_msgpack_value(item) for item in value)
    if isinstance(value, bytes):
        if field_name is not None and field_name.endswith("_payload"):
            return value
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return value
    return value
