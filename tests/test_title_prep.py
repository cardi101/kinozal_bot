from title_prep import (
    clean_release_title,
    extract_title_aliases_from_text,
    is_bad_tmdb_candidate,
    looks_like_simple_numeric_title,
    looks_like_structured_numeric_title,
    split_title_parts,
)


def test_clean_release_title_removes_year():
    result = clean_release_title("Breaking Bad 2008")
    assert "2008" not in result
    assert "Breaking Bad" in result


def test_clean_release_title_removes_quality():
    result = clean_release_title("Movie Name 1080p WEB-DL")
    assert "1080p" not in result.lower()
    assert "web" not in result.lower()


def test_clean_release_title_removes_brackets():
    result = clean_release_title("Title [Some Info] (2023)")
    assert "[" not in result
    assert "(" not in result


def test_clean_release_title_empty():
    assert clean_release_title("") == ""
    assert clean_release_title(None) == ""


def test_is_bad_tmdb_candidate_tech_only():
    assert is_bad_tmdb_candidate("1080p") is True
    assert is_bad_tmdb_candidate("WEBRip") is True
    assert is_bad_tmdb_candidate("mp3") is True
    assert is_bad_tmdb_candidate("FLAC") is True


def test_is_bad_tmdb_candidate_empty():
    assert is_bad_tmdb_candidate("") is True
    assert is_bad_tmdb_candidate(None) is True


def test_is_bad_tmdb_candidate_valid_title():
    assert is_bad_tmdb_candidate("Breaking Bad") is False
    assert is_bad_tmdb_candidate("Inception") is False


def test_is_bad_tmdb_candidate_episode_info():
    assert is_bad_tmdb_candidate("5 серий из 10") is True
    assert is_bad_tmdb_candidate("1 сезон") is True


def test_looks_like_structured_numeric():
    assert looks_like_structured_numeric_title("12-34-56") is True
    assert looks_like_structured_numeric_title("1-2-3") is True
    assert looks_like_structured_numeric_title("Breaking Bad") is False
    assert looks_like_structured_numeric_title("") is False


def test_looks_like_simple_numeric_title() -> None:
    assert looks_like_simple_numeric_title("180") is True
    assert looks_like_simple_numeric_title("12") is True
    assert looks_like_simple_numeric_title("1080") is False
    assert looks_like_simple_numeric_title("1917") is False


def test_is_bad_tmdb_candidate_allows_short_numeric_title_but_not_tech_resolution() -> None:
    assert is_bad_tmdb_candidate("180") is False
    assert is_bad_tmdb_candidate("720") is True
    assert is_bad_tmdb_candidate("1080") is True


def test_split_title_parts_ru_en():
    ru, en = split_title_parts("Во все тяжкие / Breaking Bad")
    assert "тяжкие" in ru.lower()
    assert "breaking" in en.lower()


def test_split_title_parts_en_only():
    ru, en = split_title_parts("Breaking Bad")
    assert en != ""
    assert "breaking" in en.lower()


def test_split_title_parts_short_numeric_title() -> None:
    ru, en = split_title_parts("180 / 180 / 2026 / СТ / WEBRip (1080p)")
    assert ru == ""
    assert en == "180"


def test_split_title_parts_empty():
    ru, en = split_title_parts("")
    assert ru == ""
    assert en == ""


def test_extract_title_aliases_ignores_audio_studio_parentheses() -> None:
    aliases = extract_title_aliases_from_text(
        "Я вернулась! Не помешаю? (1-2 серии из 12) / Tadaima, Ojama Saremasu! / 2026 / ЛМ (Dream Cast, DreamyVoice), СТ / HEVC / WEBRip (1080p)"
    )

    assert "Dream Cast, DreamyVoice" not in aliases
    assert "Tadaima, Ojama Saremasu!" not in aliases


def test_clean_release_title_keeps_only_title_segments_for_release_line() -> None:
    assert clean_release_title(
        "Мэтлок (2 сезон: 1-13 серии из 16) / Matlock / 2025 / ПМ (TVShows) / WEB-DL (1080p)"
    ) == "Мэтлок / Matlock"
