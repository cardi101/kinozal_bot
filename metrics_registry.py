from typing import Mapping

from prometheus_client import CollectorRegistry, Counter, Gauge, generate_latest


def _set_counter(registry: CollectorRegistry, name: str, documentation: str, value: float) -> None:
    metric = Counter(name, documentation, registry=registry)
    if value > 0:
        metric.inc(value)


def _set_gauge(registry: CollectorRegistry, name: str, documentation: str, value: float) -> None:
    Gauge(name, documentation, registry=registry).set(value)


def build_metrics_payload(
    *,
    database_up: bool,
    users_total: int,
    subscriptions_enabled: int,
    source_fail_streak: int,
    source_last_success_at: int,
    source_last_failed_at: int,
    source_status: str,
    extra_counters: Mapping[str, tuple[str, float]] | None = None,
    extra_gauges: Mapping[str, tuple[str, float]] | None = None,
) -> bytes:
    registry = CollectorRegistry()

    _set_gauge(registry, "kinozal_bot_database_up", "Database health as 1=up 0=down", 1 if database_up else 0)
    _set_gauge(registry, "kinozal_bot_users_total", "Total users in database", users_total)
    _set_gauge(registry, "kinozal_bot_enabled_subscriptions_total", "Enabled subscriptions", subscriptions_enabled)
    _set_gauge(registry, "kinozal_bot_source_fail_streak", "Consecutive source failures", source_fail_streak)

    Gauge(
        "kinozal_bot_source_status",
        "Source status as 1=ok 0=non-ok",
        labelnames=("status",),
        registry=registry,
    ).labels(status=source_status).set(1 if source_status == "ok" else 0)

    _set_gauge(
        registry,
        "kinozal_bot_source_last_success_at",
        "Last successful source cycle timestamp",
        source_last_success_at,
    )
    _set_gauge(
        registry,
        "kinozal_bot_source_last_failed_at",
        "Last failed source cycle timestamp",
        source_last_failed_at,
    )

    for name, (documentation, value) in (extra_counters or {}).items():
        _set_counter(registry, name, documentation, value)

    for name, (documentation, value) in (extra_gauges or {}).items():
        _set_gauge(registry, name, documentation, value)

    return generate_latest(registry)
