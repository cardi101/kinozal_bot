from tmdb_aliases import title_search_candidates


def test_title_search_candidates_keep_re_zero_whole_and_skip_short_colon_fragments() -> None:
    source_title = (
        "Жизнь в альтернативном мире с нуля (4 сезон: 1-2 серии из 18) / "
        "Re: Zero / 2026 / ДБ (AniStar), ЛМ (AniLibria), СТ / WEB-DL (1080p)"
    )

    candidates = title_search_candidates(source_title, "")

    assert "Re: Zero" in candidates
    assert "Re" not in candidates
    assert "Zero" not in candidates
    assert "Link Click" not in candidates
    assert "Shiguang Dailiren" not in candidates


def test_title_search_candidates_skip_generic_prefixes_for_long_colon_title() -> None:
    source_title = (
        "Легенда об Аанге: Последний маг воздуха / "
        "The Legend of Aang: The Last Airbender / 2026 / "
        "2 x ДБ, ПМ, ЛМ, СТ / WEB-DL (1080p)"
    )

    candidates = title_search_candidates(source_title, "")

    assert "The Legend of Aang: The Last Airbender" in candidates
    assert "The Legend of Aang" in candidates
    assert "The Last Airbender" in candidates
    assert "The Legend" not in candidates
    assert "The Legend of" not in candidates
    assert "The Legend of Aang The" not in candidates
