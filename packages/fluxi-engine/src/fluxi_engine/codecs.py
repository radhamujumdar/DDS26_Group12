"""Binary payload helpers for the Fluxi engine."""

from __future__ import annotations

import base64
from typing import Any

from msgspec import msgpack


def packb(payload: Any) -> bytes:
    """Encode a Python value as msgpack bytes."""

    return msgpack.encode(payload)


def unpackb(payload: bytes | bytearray | memoryview | None) -> Any:
    """Decode msgpack bytes into a Python value."""

    if payload is None:
        return None
    return msgpack.decode(bytes(payload))


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
