from keyboards import match_candidates_kb, match_review_kb


def test_match_candidates_keyboard_builds_pick_buttons() -> None:
    markup = match_candidates_kb(
        "2135465",
        [
            {"tmdb_id": 1226863, "media_type": "movie", "title": "The Super Mario Galaxy Movie"},
            {"tmdb_id": 980489, "media_type": "tv", "title": "The Super Mario Galaxy"},
        ],
    )

    rows = markup.inline_keyboard
    assert len(rows) == 2
    assert rows[0][0].callback_data == "matchpick:2135465:1226863:movie"
    assert rows[1][0].callback_data == "matchpick:2135465:980489:tv"


def test_match_review_keyboard_includes_no_match_and_force_actions() -> None:
    markup = match_review_kb("2135465", has_tmdb_match=True)

    rows = markup.inline_keyboard
    assert rows[0][0].callback_data == "matchreview:approve:2135465"
    assert rows[0][1].callback_data == "matchreview:reject:2135465"
    assert rows[1][0].callback_data == "matchreview:no_match:2135465"
    assert rows[1][1].callback_data == "matchreview:force:2135465"
