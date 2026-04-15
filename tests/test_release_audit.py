from release_audit import build_missing_progress_candidates


def test_build_missing_progress_candidates_marks_historical_gap() -> None:
    candidates = build_missing_progress_candidates(
        item_rows=[
            {
                "kinozal_id": "2128422",
                "source_title": "Reggie 1-7",
                "source_episode_progress": "1 сезон: 1-7 серии из 10",
                "created_at": 100,
            },
            {
                "kinozal_id": "2128422",
                "source_title": "Reggie 1-10",
                "source_episode_progress": "1 сезон: 1-10 серии из 10",
                "created_at": 200,
            },
        ],
        observation_rows=[
            {
                "kinozal_id": "2128422",
                "source_title": "Reggie 1-8",
                "episode_progress": "1 сезон: 1-8 серии из 10",
                "poll_ts": 150,
            }
        ],
        delivery_rows=[
            {
                "kinozal_id": "2128422",
                "delivery_count": 14,
                "delivery_users": 4,
                "last_delivered_at": 140,
            }
        ],
    )

    assert len(candidates) == 1
    assert candidates[0]["gap_kind"] == "historical_gap"
    assert candidates[0]["highest_known_below_observed"] == "1 сезон: 1-7 серии из 10"
    assert candidates[0]["current_highest_known"] == "1 сезон: 1-10 серии из 10"


def test_build_missing_progress_candidates_marks_latest_gap() -> None:
    candidates = build_missing_progress_candidates(
        item_rows=[
            {
                "kinozal_id": "2131779",
                "source_title": "Rooster 1-4",
                "source_episode_progress": "1 сезон: 1-4 серии из 10",
                "created_at": 100,
            }
        ],
        observation_rows=[
            {
                "kinozal_id": "2131779",
                "source_title": "Rooster 1-5",
                "episode_progress": "1 сезон: 1-5 серии из 10",
                "poll_ts": 150,
            }
        ],
        delivery_rows=[
            {
                "kinozal_id": "2131779",
                "delivery_count": 21,
                "delivery_users": 4,
                "last_delivered_at": 140,
            }
        ],
    )

    assert len(candidates) == 1
    assert candidates[0]["gap_kind"] == "latest_gap"
    assert candidates[0]["highest_known_below_observed"] == "1 сезон: 1-4 серии из 10"
    assert candidates[0]["current_highest_known"] == "1 сезон: 1-4 серии из 10"
