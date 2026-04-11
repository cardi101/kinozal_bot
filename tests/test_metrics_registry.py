from metrics_registry import build_metrics_payload


def test_build_metrics_payload_includes_base_and_extra_metrics() -> None:
    payload = build_metrics_payload(
        database_up=True,
        users_total=7,
        subscriptions_enabled=11,
        source_fail_streak=0,
        source_last_success_at=123,
        source_last_failed_at=122,
        source_status="ok",
        extra_counters={
            "kinozal_bot_worker_cycles_total": ("Total completed worker cycles", 5),
        },
        extra_gauges={
            "kinozal_bot_worker_cycle_duration_seconds": ("Last cycle duration", 7.25),
        },
    ).decode("utf-8")

    assert "kinozal_bot_database_up 1.0" in payload
    assert "kinozal_bot_users_total 7.0" in payload
    assert 'kinozal_bot_source_status{status="ok"} 1.0' in payload
    assert "kinozal_bot_worker_cycles_total 5.0" in payload
    assert "kinozal_bot_worker_cycle_duration_seconds 7.25" in payload
