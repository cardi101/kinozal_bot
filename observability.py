import logging
from typing import Any

import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration


def init_sentry(cfg: Any, service_name: str, log: logging.Logger) -> bool:
    dsn = str(getattr(cfg, "sentry_dsn", "") or "").strip()
    if not dsn:
        return False

    logging_integration = LoggingIntegration(
        level=logging.INFO,
        event_level=logging.ERROR,
    )
    sentry_sdk.init(
        dsn=dsn,
        environment=str(getattr(cfg, "sentry_environment", "") or "production"),
        release=str(getattr(cfg, "sentry_release", "") or None),
        traces_sample_rate=float(getattr(cfg, "sentry_traces_sample_rate", 0.0) or 0.0),
        integrations=[logging_integration],
        send_default_pii=False,
    )
    sentry_sdk.set_tag("service", service_name)
    log.info(
        "Sentry enabled service=%s environment=%s traces_sample_rate=%s",
        service_name,
        getattr(cfg, "sentry_environment", "production"),
        getattr(cfg, "sentry_traces_sample_rate", 0.0),
    )
    return True
