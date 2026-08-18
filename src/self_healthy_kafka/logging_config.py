from __future__ import annotations

import logging
import sys

from pythonjsonlogger import jsonlogger

from self_healthy_kafka.config import cfg


class _SafeStreamHandler(logging.StreamHandler):
    def emit(self, record):
        try:
            msg = self.format(record)
            self.stream.write(msg + self.terminator)
            try:
                self.stream.flush()
            except OSError:
                pass
        except OSError as e:
            if e.errno != 9:
                self.handleError(record)
        except Exception:
            self.handleError(record)


def _resolve_log_level() -> int:
    """Map LOG_LEVEL env (e.g. 'DEBUG', 'INFO') to a numeric level."""
    raw = cfg.logging.log_level.upper().strip()
    return getattr(logging, raw, logging.INFO)


def setup_logging() -> None:
    if sys.platform == "win32":
        reconfigure = getattr(sys.stdout, "reconfigure", None)
        if reconfigure:
            reconfigure(encoding="utf-8")

    json_formatter = jsonlogger.JsonFormatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
        rename_fields={
            "asctime": "timestamp",
            "levelname": "level",
            "name": "logger",
        },
        datefmt="%Y-%m-%dT%H:%M:%S%z",
        json_ensure_ascii=False,
        json_default=str,
    )

    stdout_handler = _SafeStreamHandler(sys.stdout)
    stdout_handler.setFormatter(json_formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(_resolve_log_level())
    root.addHandler(stdout_handler)

    # Library loggers always at WARNING — too noisy at INFO/DEBUG.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    # One synchronous line so you can immediately verify the file works.
    logging.getLogger(__name__).info(
        "logging initialized",
        extra={
            "event": "logging_initialized",
            "log_level": cfg.logging.log_level,
        },
    )

