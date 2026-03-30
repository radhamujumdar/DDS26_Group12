from __future__ import annotations

from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
SDK_SRC = REPO_ROOT / "packages" / "fluxi-sdk" / "src"
ENGINE_SRC = REPO_ROOT / "packages" / "fluxi-engine" / "src"
ORDER_DIR = REPO_ROOT / "order"


def ensure_repo_paths() -> None:
    for path in (SDK_SRC, ENGINE_SRC, ORDER_DIR):
        resolved = str(path)
        if resolved not in sys.path:
            sys.path.insert(0, resolved)


ensure_repo_paths()
