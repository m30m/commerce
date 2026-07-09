"""Structured (JSON) logging setup shared by every service.

Each log record is emitted as a single JSON line on stdout with a stable set of
fields (time, level, logger, service, message) plus any structured ``extra``
fields passed at the call site. This is what the log collector parses to attach
labels, and what makes queries like ``{service="cart"} | json | status>=500``
work in Grafana.
"""
import json
import logging
import sys
from datetime import datetime, timezone

# Attribute names that already live on every LogRecord; everything else in
# record.__dict__ is treated as a caller-supplied structured field.
_RESERVED = set(
    vars(logging.LogRecord("", 0, "", 0, "", None, None)).keys()
) | {"message", "asctime", "taskName"}


class JsonFormatter(logging.Formatter):
    def __init__(self, service: str) -> None:
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "time": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "service": self.service,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack_info"] = self.formatStack(record.stack_info)
        return json.dumps(payload, default=str)


def configure_logging(service_name: str, level: str = "INFO") -> logging.Logger:
    """Route the root logger (and uvicorn's loggers) through JSON output.

    uvicorn configures its own logging before importing the app, so this runs
    afterwards and takes over: the root logger gets a single JSON handler and
    uvicorn's loggers propagate into it. uvicorn's plaintext access log is
    disabled in favour of the structured access log emitted by the request
    middleware.
    """
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(service_name))

    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)

    for name in ("uvicorn", "uvicorn.error"):
        logger = logging.getLogger(name)
        logger.handlers = []
        logger.propagate = True

    access = logging.getLogger("uvicorn.access")
    access.handlers = []
    access.disabled = True

    return logging.getLogger(service_name)
