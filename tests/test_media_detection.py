from media_detection import is_non_video_release, detect_media_type
from source_categories import source_category_is_non_video


def test_non_video_music():
    assert is_non_video_release("Artist - Album / Pop / MP3") is True
    assert is_non_video_release("Discography FLAC") is True


def test_non_video_software():
    assert is_non_video_release("Photoshop 2024 x64 Windows installer") is True


def test_non_video_games():
    assert is_non_video_release("Cyberpunk 2077 PC(Windows) Repack RPG") is True


def test_non_video_books():
    assert is_non_video_release("Книга - Автор / PDF / FB2") is True
    assert is_non_video_release("Терри Гудкайнд - Цикл Меч Истины / M4B") is True


def test_non_video_clips():
    assert is_non_video_release("Artist - Track / Видеоклипы / WEBRip (1080p)") is True


def test_non_video_sports():
    assert is_non_video_release("НХЛ / NHL 2024 Матч Финал") is True


def test_video_release():
    assert is_non_video_release("Breaking Bad S01E01 1080p WEB-DL") is False
    assert is_non_video_release("Inception 2010 BDRip") is False


def test_detect_media_type_tv():
    assert detect_media_type("Breaking Bad S01E01") == "tv"
    assert detect_media_type("Сериал 1 сезон 5 серия") == "tv"


def test_detect_media_type_movie():
    assert detect_media_type("Inception 2010 BDRip 1080p") == "movie"


def test_detect_media_type_other():
    assert detect_media_type("Artist - Album / Pop / MP3") == "other"


def test_source_category_is_non_video_for_non_target_buckets():
    assert source_category_is_non_video("1", "Другое - Видеоклипы") is True
    assert source_category_is_non_video("2", "Другое - АудиоКниги") is True
