#!/usr/bin/env python3
"""winbridge: Windows-first launcher for the bridge protocol.

This is a thin wrapper around bridge.py that keeps the core protocol the same
but makes the default attach mode a local TCP listener.
"""

from __future__ import annotations

import sys
from typing import List, Optional

import bridge


def normalize_argv(argv: List[str]) -> List[str]:
    if not argv:
        return argv

    cmd = argv[0]
    if cmd != "attach":
        return argv

    if "--transport" in argv:
        return argv

    if len(argv) < 2:
        return [argv[0], "--transport", "tcp"]
    return [argv[0], argv[1], "--transport", "tcp", *argv[2:]]


def main(argv: Optional[List[str]] = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    return bridge.main(normalize_argv(list(argv)))


if __name__ == "__main__":
    raise SystemExit(main())
