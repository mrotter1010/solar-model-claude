"""Exclude scratch directory from pytest collection.

Scratch scripts contain top-level executable code (HTTP calls, timing tests)
that should not run during normal test collection.
"""

collect_ignore_glob = ["*.py"]
