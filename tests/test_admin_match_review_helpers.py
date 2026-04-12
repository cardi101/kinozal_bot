import asyncio

from admin_match_review_helpers import (
    build_match_review_alert,
    item_requires_match_review,
    notify_admins_about_match_review,
)
from config import CFG


class _DummyBot:
    def __init__(self, fail_for: int | None = None) -> None:
        self.fail_for = fail_for
        self.sent_to: list[int] = []

    async def send_message(self, chat_id: int, *args, **kwargs) -> None:
        if self.fail_for is not None and chat_id == self.fail_for:
            raise RuntimeError("send failed")
        self.sent_to.append(chat_id)


def test_notify_admins_about_match_review_returns_successful_send_count() -> None:
    original_admin_ids = CFG.admin_ids
    CFG.admin_ids = (101, 202)
    bot = _DummyBot(fail_for=202)
    item = {
        "kinozal_id": "2130471",
        "source_title": "Seiren",
        "tmdb_id": 123,
        "tmdb_title": "Seiren",
        "tmdb_match_confidence": "low",
        "tmdb_match_path": "search",
        "tmdb_match_evidence": "weak title overlap",
    }

    try:
        sent_count = asyncio.run(notify_admins_about_match_review(bot, item, affected_users=0))
    finally:
        CFG.admin_ids = original_admin_ids

    assert sent_count == 1
    assert bot.sent_to == [101]


def test_notify_admins_about_match_review_returns_zero_without_admins() -> None:
    original_admin_ids = CFG.admin_ids
    CFG.admin_ids = ()
    bot = _DummyBot()
    item = {
        "kinozal_id": "1943921",
        "source_title": "Zeder",
        "tmdb_match_confidence": "unmatched",
        "tmdb_match_path": "search",
        "tmdb_match_evidence": "no match",
    }

    try:
        sent_count = asyncio.run(notify_admins_about_match_review(bot, item, affected_users=0))
    finally:
        CFG.admin_ids = original_admin_ids

    assert sent_count == 0
    assert bot.sent_to == []


def test_notify_admins_about_match_review_skips_requested_admin_ids() -> None:
    original_admin_ids = CFG.admin_ids
    CFG.admin_ids = (101, 202)
    bot = _DummyBot()
    item = {
        "kinozal_id": "1943921",
        "source_title": "Zeder",
        "tmdb_match_confidence": "low",
        "tmdb_match_path": "search",
        "tmdb_match_evidence": "weak overlap",
    }

    try:
        sent_count = asyncio.run(
            notify_admins_about_match_review(bot, item, affected_users=0, skip_admin_ids={101})
        )
    finally:
        CFG.admin_ids = original_admin_ids

    assert sent_count == 1
    assert bot.sent_to == [202]


def test_build_match_review_alert_escapes_override_placeholders() -> None:
    text = build_match_review_alert(
        {
            "kinozal_id": "1943921",
            "source_title": "Zeder",
            "tmdb_id": 123,
            "tmdb_title": "Zeder",
            "tmdb_match_confidence": "low",
            "tmdb_match_path": "search",
            "tmdb_match_evidence": "weak overlap",
        },
        affected_users=0,
    )

    assert "&lt;tmdb_id&gt;" in text
    assert "&lt;movie|tv&gt;" in text
    assert "<tmdb_id>" not in text


def test_item_requires_match_review_only_for_low_with_tmdb_id() -> None:
    original_enabled = CFG.match_review_enabled
    CFG.match_review_enabled = True
    try:
        assert item_requires_match_review({"tmdb_match_confidence": "low", "tmdb_id": 123}) is True
        assert item_requires_match_review({"tmdb_match_confidence": "medium", "tmdb_id": 123}) is False
        assert item_requires_match_review({"tmdb_match_confidence": "unmatched", "kinozal_id": "123"}) is False
        assert item_requires_match_review({"tmdb_match_confidence": "low"}) is False
    finally:
        CFG.match_review_enabled = original_enabled


def test_item_requires_match_review_disabled_by_default_flag() -> None:
    original_enabled = CFG.match_review_enabled
    CFG.match_review_enabled = False
    try:
        assert item_requires_match_review({"tmdb_match_confidence": "low", "tmdb_id": 123}) is False
    finally:
        CFG.match_review_enabled = original_enabled
