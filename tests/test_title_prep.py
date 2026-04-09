from title_prep import (
    clean_release_title,
    is_bad_tmdb_candidate,
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


def test_split_title_parts_ru_en():
    ru, en = split_title_parts("Во все тяжкие / Breaking Bad")
    assert "тяжкие" in ru.lower()
    assert "breaking" in en.lower()


def test_split_title_parts_en_only():
    ru, en = split_title_parts("Breaking Bad")
    assert en != ""
    assert "breaking" in en.lower()


def test_split_title_parts_empty():
    ru, en = split_title_parts("")
    assert ru == ""
    assert en == ""
