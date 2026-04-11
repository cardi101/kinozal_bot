import logging
import os

from aiogram import Bot

from config import CFG
from services.alertmanager_webhook_service import AlertmanagerWebhookService


def configure_logging() -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    return logging.getLogger("kinozal-alert-webhook")


def build_alertmanager_webhook_service() -> AlertmanagerWebhookService:
    configure_logging()
    return AlertmanagerWebhookService(Bot(CFG.bot_token))
