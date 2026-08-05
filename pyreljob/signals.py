"""Signal helpers for graceful worker/scheduler shutdown."""

from __future__ import annotations

import signal
import threading
from collections.abc import Callable


def install_shutdown_handler(callback: Callable[[], None]) -> None:
    """Arrange for ``callback`` to run on SIGINT/SIGTERM.

    Signal handlers can only be registered from the main thread; anywhere else
    this is a no-op (the run loop's ``KeyboardInterrupt`` catch remains a
    fallback).
    """
    if threading.current_thread() is not threading.main_thread():
        return

    def _handle(_signum: int, _frame: object) -> None:
        callback()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _handle)
        except (ValueError, OSError):
            pass
