from prometheus_client import CollectorRegistry, Gauge, generate_latest


def build_metrics_payload(
    *,
    database_up: bool,
    users_total: int,
    subscriptions_enabled: int,
    source_fail_streak: int,
    source_last_success_at: int,
    source_last_failed_at: int,
    source_status: str,
) -> bytes:
    registry = CollectorRegistry()

    Gauge(
        "kinozal_bot_database_up",
        "Database health as 1=up 0=down",
        registry=registry,
    ).set(1 if database_up else 0)

    Gauge(
        "kinozal_bot_users_total",
        "Total users in database",
        registry=registry,
    ).set(users_total)

    Gauge(
        "kinozal_bot_enabled_subscriptions_total",
        "Enabled subscriptions",
        registry=registry,
    ).set(subscriptions_enabled)

    Gauge(
        "kinozal_bot_source_fail_streak",
        "Consecutive source failures",
        registry=registry,
    ).set(source_fail_streak)

    Gauge(
        "kinozal_bot_source_status",
        "Source status as 1=ok 0=non-ok",
        labelnames=("status",),
        registry=registry,
    ).labels(status=source_status).set(1 if source_status == "ok" else 0)

    Gauge(
        "kinozal_bot_source_last_success_at",
        "Last successful source cycle timestamp",
        registry=registry,
    ).set(source_last_success_at)

    Gauge(
        "kinozal_bot_source_last_failed_at",
        "Last failed source cycle timestamp",
        registry=registry,
    ).set(source_last_failed_at)

    return generate_latest(registry)
