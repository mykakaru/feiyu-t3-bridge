import sys
from pathlib import Path
_TESTS = str(Path(__file__).resolve().parent)
_BRIDGE = str(Path(__file__).resolve().parent.parent / 'bridge')
for _p in (_BRIDGE, _TESTS):
    if _p not in sys.path:
        sys.path.insert(0, _p)
