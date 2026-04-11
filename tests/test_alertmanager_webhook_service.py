from aiogram import Bot

from services.alertmanager_webhook_service import AlertmanagerWebhookService


def test_build_message_contains_alert_summary() -> None:
    service = AlertmanagerWebhookService(Bot("123456:TESTTOKEN"))
    message = service.build_message(
        {
            "status": "firing",
            "receiver": "telegram_admins",
            "commonLabels": {"severity": "critical"},
            "alerts": [
                {
                    "status": "firing",
                    "labels": {"alertname": "KinozalApiDown"},
                    "annotations": {
                        "summary": "Kinozal API is down",
                        "description": "Prometheus cannot scrape the API.",
                    },
                }
            ],
        }
    )

    assert "Alertmanager: FIRING" in message
    assert "KinozalApiDown" in message
    assert "Kinozal API is down" in message
    assert "telegram_admins" in message
