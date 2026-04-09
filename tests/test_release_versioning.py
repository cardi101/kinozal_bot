from release_versioning import parse_episode_progress, extract_kinozal_id


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
