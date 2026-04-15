from release_versioning import (
    classify_episode_progress_change,
    compare_episode_progress,
    extract_kinozal_id,
    parse_episode_progress,
)


def test_parse_episode_progress_series():
    assert parse_episode_progress("1-10 серий из 16") is not None
    assert parse_episode_progress("5 серия из 8") is not None


def test_parse_episode_progress_season():
    result = parse_episode_progress("2 сезон: 1-5 серий из 10")
    assert result is not None
    assert "сезон" in result


def test_parse_episode_progress_sxxexx():
    result = parse_episode_progress("S02E05")
    assert result is not None
    assert "S02E05" in result.upper()


def test_parse_episode_progress_range():
    result = parse_episode_progress("S01 E01 - E10")
    assert result is not None


def test_parse_episode_progress_issues():
    result = parse_episode_progress("15 выпусков из 20")
    assert result is not None
    assert "выпуск" in result


def test_parse_episode_progress_n_of_m():
    result = parse_episode_progress("5 из 10")
    assert result is not None
    assert "из" in result


def test_parse_episode_progress_none():
    assert parse_episode_progress("Just a movie title") is None
    assert parse_episode_progress("") is None


def test_extract_kinozal_id():
    assert extract_kinozal_id("https://kinozal.tv/details.php?id=12345") == "12345"
    assert extract_kinozal_id("no id here") is None
    assert extract_kinozal_id("") is None


def test_compare_episode_progress_range_growth():
    assert compare_episode_progress("2 сезон: 1-10 серии из 12", "2 сезон: 1-9 серии из 12") == 1
    assert classify_episode_progress_change("2 сезон: 1-9 серии из 12", "2 сезон: 1-10 серии из 12") == "up"


def test_compare_episode_progress_regression():
    assert compare_episode_progress("2 сезон: 1-8 серии из 12", "2 сезон: 1-9 серии из 12") == -1
    assert classify_episode_progress_change("2 сезон: 1-9 серии из 12", "2 сезон: 1-8 серии из 12") == "down"


def test_compare_episode_progress_unknown():
    assert compare_episode_progress("Just a movie title", "2 сезон: 1-9 серии из 12") is None
    assert classify_episode_progress_change("2 сезон: 1-9 серии из 12", "Just a movie title") == "unknown"


def test_compare_episode_progress_ignores_missing_season_prefix() -> None:
    assert compare_episode_progress("1-3 серии", "1 сезон: 1-2 серии из 25") == 1
    assert compare_episode_progress("1 серия из 12", "1 сезон: 1 серия из 12") == 0


def test_compare_episode_progress_marks_incompatible_totals_as_unknown() -> None:
    assert compare_episode_progress("1 сезон: 1-6 серии из 6", "1 сезон: 13 серии") is None
    assert classify_episode_progress_change("1 сезон: 13 серии", "1 сезон: 1-6 серии из 6") == "unknown"
