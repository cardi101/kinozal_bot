from scripts import backfill_parsed_release_fields as backfill_module


class _ParsedRelease:
    def __init__(self, payload: str) -> None:
        self.payload = payload

    def to_json(self) -> str:
        return self.payload


def test_build_recomputed_payload_refreshes_from_fresh_parsed_release(monkeypatch) -> None:
    seen = {}

    def _fake_parse_release_title(source_title: str, media_type: str) -> _ParsedRelease:
        assert source_title == "Title / 2026 / WEB-DL (1080p)"
        assert media_type == "movie"
        return _ParsedRelease('{"fresh":true}')

    def _fake_refresh_item_version_fields(payload):
        seen["parsed_release_json"] = payload["parsed_release_json"]
        return {"version_signature": "new-signature"}

    monkeypatch.setattr(backfill_module, "parse_release_title", _fake_parse_release_title)
    monkeypatch.setattr(backfill_module, "refresh_item_version_fields", _fake_refresh_item_version_fields)

    parsed_release_json, refreshed = backfill_module._build_recomputed_payload(
        {
            "id": 1,
            "source_uid": "kinozal:1",
            "media_type": "movie",
            "source_title": "Title / 2026 / WEB-DL (1080p)",
            "parsed_release_json": '{"stale":true}',
        }
    )

    assert parsed_release_json == '{"fresh":true}'
    assert seen["parsed_release_json"] == '{"fresh":true}'
    assert refreshed["version_signature"] == "new-signature"
