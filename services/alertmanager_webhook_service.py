import html
import logging
from typing import Any, Dict, List

from aiogram import Bot

from service_helpers import send_admins_text

log = logging.getLogger("kinozal-alert-webhook")


class AlertmanagerWebhookService:
    def __init__(self, bot: Bot) -> None:
        self.bot = bot

    @staticmethod
    def _normalize_alerts(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        alerts = payload.get("alerts")
        if isinstance(alerts, list):
            return [alert for alert in alerts if isinstance(alert, dict)]
        return []

    def build_message(self, payload: Dict[str, Any]) -> str:
        status = str(payload.get("status") or "unknown").lower()
        alerts = self._normalize_alerts(payload)
        firing = sum(1 for alert in alerts if str(alert.get("status") or status).lower() == "firing")
        resolved = sum(1 for alert in alerts if str(alert.get("status") or status).lower() == "resolved")
        emoji = "🔥" if status == "firing" else "✅" if status == "resolved" else "ℹ️"

        lines = [
            f"{emoji} <b>Alertmanager: {html.escape(status.upper())}</b>",
            f"Alerts: {len(alerts)}",
        ]
        if firing:
            lines.append(f"Firing: {firing}")
        if resolved:
            lines.append(f"Resolved: {resolved}")

        common_labels = payload.get("commonLabels") or {}
        severity = str(common_labels.get("severity") or "").strip()
        if severity:
            lines.append(f"Severity: <code>{html.escape(severity)}</code>")

        receiver = str(payload.get("receiver") or "").strip()
        if receiver:
            lines.append(f"Receiver: <code>{html.escape(receiver)}</code>")

        max_alerts = 5
        for index, alert in enumerate(alerts[:max_alerts], start=1):
            labels = alert.get("labels") or {}
            annotations = alert.get("annotations") or {}
            alert_name = str(labels.get("alertname") or f"Alert {index}")
            summary = str(annotations.get("summary") or "").strip()
            description = str(annotations.get("description") or "").strip()
            alert_status = str(alert.get("status") or status).upper()

            lines.append("")
            lines.append(f"<b>{index}. {html.escape(alert_name)}</b> [{html.escape(alert_status)}]")
            if summary:
                lines.append(html.escape(summary))
            if description:
                lines.append(html.escape(description))

        if len(alerts) > max_alerts:
            lines.append("")
            lines.append(f"... и ещё {len(alerts) - max_alerts} alert(s)")

        return "\n".join(lines)

    async def handle_webhook(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        alerts = self._normalize_alerts(payload)
        message = self.build_message(payload)
        if alerts:
            await send_admins_text(self.bot, message)
            log.info("Forwarded alertmanager payload alerts=%s status=%s", len(alerts), payload.get("status"))
        else:
            log.info("Received empty alertmanager payload")
        return {
            "ok": True,
            "alerts": len(alerts),
            "status": str(payload.get("status") or "unknown"),
        }

    async def close(self) -> None:
        await self.bot.session.close()
