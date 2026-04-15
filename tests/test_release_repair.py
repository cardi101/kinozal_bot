from release_repair import group_users_by_kinozal, select_repair_candidates, summarize_repair_statuses


def test_select_repair_candidates_keeps_latest_gap_with_users_by_default() -> None:
    candidates = [
        {"kinozal_id": "1", "gap_kind": "latest_gap", "delivery_users": 2},
        {"kinozal_id": "2", "gap_kind": "historical_gap", "delivery_users": 3},
        {"kinozal_id": "3", "gap_kind": "latest_gap", "delivery_users": 0},
    ]

    result = select_repair_candidates(candidates)

    assert [row["kinozal_id"] for row in result] == ["1"]


def test_group_users_by_kinozal_sorts_by_recent_delivery() -> None:
    rows = [
        {"kinozal_id": "2128422", "tg_user_id": 2, "last_delivered_at": 100},
        {"kinozal_id": "2128422", "tg_user_id": 1, "last_delivered_at": 200},
        {"kinozal_id": "2130000", "tg_user_id": 3, "last_delivered_at": 150},
    ]

    grouped = group_users_by_kinozal(rows)

    assert [row["tg_user_id"] for row in grouped["2128422"]] == [1, 2]
    assert [row["tg_user_id"] for row in grouped["2130000"]] == [3]


def test_summarize_repair_statuses_counts_rows() -> None:
    rows = [
        {"status": "ready"},
        {"status": "skipped"},
        {"status": "ready"},
    ]

    assert summarize_repair_statuses(rows) == {"ready": 2, "skipped": 1}
