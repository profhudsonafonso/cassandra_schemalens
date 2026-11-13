# Copied V8 modules (reused/methodology) use absolute imports `methodology.<x>`; resolve them to the copies.
import sys as _sys
from pathlib import Path as _P

_r = str(_P(__file__).resolve().parents[1] / "reused")
if _r not in _sys.path:
    _sys.path.append(_r)
