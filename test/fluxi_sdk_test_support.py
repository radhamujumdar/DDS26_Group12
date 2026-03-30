from __future__ import annotations

from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
SDK_SRC = REPO_ROOT / "packages" / "fluxi-sdk" / "src"
ENGINE_SRC = REPO_ROOT / "packages" / "fluxi-engine" / "src"
ORDER_DIR = REPO_ROOT / "order"


def _purge_preloaded_modules(prefix: str) -> None:
    for name in tuple(sys.modules):
        if name == prefix or name.startswith(f"{prefix}."):
            del sys.modules[name]


def ensure_repo_paths() -> None:
    for module_prefix in ("fluxi_sdk", "fluxi_engine"):
        _purge_preloaded_modules(module_prefix)
    for path in reversed((SDK_SRC, ENGINE_SRC, ORDER_DIR)):
        resolved = str(path)
        if resolved in sys.path:
            sys.path.remove(resolved)
        sys.path.insert(0, resolved)


ensure_repo_paths()
