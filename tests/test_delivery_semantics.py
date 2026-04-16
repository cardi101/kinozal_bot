from datetime import datetime, timezone

from delivery_events import build_delivery_event_key, build_grouped_event_key
from quiet_hours import next_quiet_window_end_ts, quiet_window_status


def _item() -> dict:
    return {
        "id": 42,
        "kinozal_id": "2128422",
        "source_uid": "kinozal:2128422",
        "version_signature": "v1",
        "source_release_text": "WEB-DL 1080p\nПоявилась озвучка",
    }


def test_build_delivery_event_key_distinguishes_release_and_release_text() -> None:
    item = _item()

    release_key = build_delivery_event_key(1001, item, context="worker")
    release_text_key = build_delivery_event_key(1001, item, context="release_text_update", is_release_text_change=True)

    assert release_key == "release:1001:2128422:v1"
    assert release_text_key.startswith("release_text:1001:2128422:")
    assert release_text_key != release_key


def test_build_grouped_event_key_is_stable_for_same_items() -> None:
    items = [_item(), _item() | {"id": 43, "version_signature": "v2"}]

    left = build_grouped_event_key(1001, items, group_key="tmdb:77")
    right = build_grouped_event_key(1001, list(reversed(items)), group_key="tmdb:77")

    assert left == right
    assert left.startswith("grouped:1001:tmdb:77:")


def test_quiet_window_status_uses_local_timezone() -> None:
    now = datetime(2026, 4, 16, 20, 30, tzinfo=timezone.utc)

    status = quiet_window_status(22, 8, "Europe/Berlin", now=now)

    assert status["timezone"] == "Europe/Berlin"
    assert status["local_hour"] == 22
    assert status["active"] is True


def test_next_quiet_window_end_ts_respects_local_timezone() -> None:
    now = datetime(2026, 4, 16, 20, 30, tzinfo=timezone.utc)

    end_ts = next_quiet_window_end_ts(22, 8, "Europe/Berlin", now=now)

    assert end_ts == int(datetime(2026, 4, 17, 6, 0, tzinfo=timezone.utc).timestamp())
