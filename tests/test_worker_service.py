from services.worker_service import WorkerService


def test_should_emit_anomaly_alert_only_for_impacted_users() -> None:
    assert WorkerService._should_emit_anomaly_alert({}, None) is False
    assert WorkerService._should_emit_anomaly_alert({123: []}, None) is True


def test_should_emit_anomaly_alert_suppresses_duplicates() -> None:
    existing = {"id": 1, "status": "open"}
    assert WorkerService._should_emit_anomaly_alert({123: []}, existing) is False
