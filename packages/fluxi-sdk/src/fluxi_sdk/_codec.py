"""Internal payload and failure codec for the engine-backed SDK runtime."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import fields, is_dataclass
import importlib
import inspect
from types import UnionType
from typing import Any, get_args, get_origin, get_type_hints

from fluxi_engine.codecs import packb, unpackb

from .errors import RemoteExecutionError

_TYPE_MARKER = "__fluxi_type__"
_TUPLE_TYPE = "tuple"
_DATACLASS_TYPE = "dataclass"
_FAILURE_MARKER = "__fluxi_failure__"


def encode_payload(value: Any) -> bytes:
    return packb(_encode_value(value))


def decode_payload(
    payload: bytes | bytearray | memoryview | None,
    *,
    expected_type: Any | None = None,
) -> Any:
    return _decode_value(unpackb(payload), expected_type)


def encode_args_payload(args: tuple[Any, ...]) -> bytes:
    return encode_payload(args)


def decode_args_payload(
    payload: bytes | bytearray | memoryview | None,
    fn: Callable[..., Any],
) -> tuple[Any, ...]:
    raw_args = decode_payload(payload)
    if isinstance(raw_args, list):
        raw_args = tuple(raw_args)
    if not isinstance(raw_args, tuple):
        raise TypeError("Encoded workflow/activity args must decode to a tuple.")

    signature = inspect.signature(fn)
    hints = get_type_hints(fn)
    parameters = [
        parameter
        for parameter in signature.parameters.values()
        if parameter.kind
        in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
    ]

    decoded_args: list[Any] = []
    for index, value in enumerate(raw_args):
        expected_type = None
        if index < len(parameters):
            parameter = parameters[index]
            expected_type = hints.get(parameter.name, parameter.annotation)
        decoded_args.append(_decode_value(value, expected_type))
    return tuple(decoded_args)


def encode_failure_payload(exc: BaseException) -> bytes:
    return packb(
        {
            _FAILURE_MARKER: True,
            "module": exc.__class__.__module__,
            "qualname": exc.__class__.__qualname__,
            "args": _encode_value(exc.args),
            "message": str(exc),
        }
    )


def decode_failure_payload(
    payload: bytes | bytearray | memoryview | None,
    *,
    fallback_error: type[RemoteExecutionError],
) -> BaseException:
    raw = unpackb(payload)
    if not isinstance(raw, dict) or raw.get(_FAILURE_MARKER) is not True:
        raise TypeError("Failure payload is not a valid Fluxi failure envelope.")

    remote_module = _coerce_text(raw.get("module"))
    remote_qualname = _coerce_text(raw.get("qualname"))
    message = _coerce_text(raw.get("message")) or "Remote execution failed."
    decoded_args = _decode_value(raw.get("args"))
    if isinstance(decoded_args, list):
        decoded_args = tuple(decoded_args)
    if not isinstance(decoded_args, tuple):
        decoded_args = ()

    exception_type = _import_symbol(remote_module, remote_qualname)
    if (
        isinstance(exception_type, type)
        and issubclass(exception_type, BaseException)
    ):
        try:
            return exception_type(*decoded_args)
        except Exception:
            pass

    return fallback_error(
        message,
        remote_module=remote_module,
        remote_qualname=remote_qualname,
        remote_args=tuple(decoded_args),
    )


def decode_return_payload(
    payload: bytes | bytearray | memoryview | None,
    fn: Callable[..., Any],
) -> Any:
    hints = get_type_hints(fn)
    return decode_payload(payload, expected_type=hints.get("return"))


def _encode_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str, bytes)):
        return value
    if isinstance(value, list):
        return [_encode_value(item) for item in value]
    if isinstance(value, tuple):
        return {
            _TYPE_MARKER: _TUPLE_TYPE,
            "items": [_encode_value(item) for item in value],
        }
    if isinstance(value, dict):
        encoded: dict[Any, Any] = {}
        for key, item in value.items():
            encoded[_encode_key(key)] = _encode_value(item)
        return encoded
    if is_dataclass(value):
        return {
            _TYPE_MARKER: _DATACLASS_TYPE,
            "class": _qualified_name(type(value)),
            "fields": {
                field.name: _encode_value(getattr(value, field.name))
                for field in fields(type(value))
            },
        }
    raise TypeError(f"Fluxi payload codec does not support values of type {type(value)!r}.")


def _encode_key(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str, bytes)):
        return value
    raise TypeError(
        "Fluxi payload codec only supports scalar dictionary keys."
    )


def _decode_value(value: Any, expected_type: Any | None = None) -> Any:
    expected_type = _normalize_expected_type(expected_type)

    if isinstance(value, dict) and value.get(_TYPE_MARKER) == _TUPLE_TYPE:
        item_types = ()
        vararg_type = None
        if expected_type is not None:
            origin = get_origin(expected_type)
            args = get_args(expected_type)
            if origin is tuple and args:
                if len(args) == 2 and args[1] is Ellipsis:
                    vararg_type = args[0]
                else:
                    item_types = args
        items = value.get("items", [])
        decoded_items = []
        for index, item in enumerate(items):
            item_type = vararg_type
            if index < len(item_types):
                item_type = item_types[index]
            decoded_items.append(_decode_value(item, item_type))
        return tuple(decoded_items)

    if isinstance(value, dict) and value.get(_TYPE_MARKER) == _DATACLASS_TYPE:
        fields_payload = value.get("fields", {})
        target_type = expected_type
        if not (inspect.isclass(target_type) and is_dataclass(target_type)):
            target_type = _import_qualified_name(_coerce_text(value.get("class")))
        if inspect.isclass(target_type) and is_dataclass(target_type):
            return _decode_dataclass(fields_payload, target_type)
        return {
            key: _decode_value(item)
            for key, item in fields_payload.items()
        }

    origin = get_origin(expected_type)
    args = get_args(expected_type)
    if origin in (UnionType, getattr(__import__("typing"), "Union")):
        if value is None and type(None) in args:
            return None
        for option in args:
            if option is type(None):
                continue
            try:
                return _decode_value(value, option)
            except Exception:
                continue
        return value

    if inspect.isclass(expected_type) and is_dataclass(expected_type) and isinstance(value, dict):
        return _decode_dataclass(value, expected_type)

    if origin is list and isinstance(value, list):
        item_type = args[0] if args else None
        return [_decode_value(item, item_type) for item in value]

    if origin is dict and isinstance(value, dict):
        key_type = args[0] if args else None
        value_type = args[1] if len(args) > 1 else None
        return {
            _decode_value(key, key_type): _decode_value(item, value_type)
            for key, item in value.items()
        }

    if isinstance(value, list) and expected_type is tuple:
        return tuple(_decode_value(item) for item in value)

    if expected_type in (None, Any):
        return _decode_untyped(value)

    if expected_type is type(None):
        return None

    if inspect.isclass(expected_type) and isinstance(value, expected_type):
        return value

    if inspect.isclass(expected_type) and expected_type in (str, int, float, bool, bytes):
        return expected_type(value)

    return _decode_untyped(value)


def _decode_untyped(value: Any) -> Any:
    if isinstance(value, list):
        return [_decode_untyped(item) for item in value]
    if isinstance(value, dict):
        return {
            _decode_untyped(key): _decode_untyped(item)
            for key, item in value.items()
        }
    return value


def _decode_dataclass(payload: dict[str, Any], target_type: type[Any]) -> Any:
    hints = get_type_hints(target_type)
    values: dict[str, Any] = {}
    for field in fields(target_type):
        if field.name not in payload:
            continue
        values[field.name] = _decode_value(
            payload[field.name],
            hints.get(field.name, field.type),
        )
    return target_type(**values)


def _qualified_name(target: type[Any]) -> str:
    return f"{target.__module__}:{target.__qualname__}"


def _normalize_expected_type(expected_type: Any | None) -> Any | None:
    if expected_type is inspect._empty:
        return None
    return expected_type


def _import_symbol(
    module_name: str | None,
    qualname: str | None,
) -> Any | None:
    if not module_name or not qualname:
        return None
    try:
        module = importlib.import_module(module_name)
    except Exception:
        return None

    value: Any = module
    try:
        for part in qualname.split("."):
            if part == "<locals>":
                return None
            value = getattr(value, part)
    except AttributeError:
        return None
    return value


def _import_qualified_name(value: str | None) -> Any | None:
    if not value or ":" not in value:
        return None
    module_name, qualname = value.split(":", 1)
    return _import_symbol(module_name, qualname)


def _coerce_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)

