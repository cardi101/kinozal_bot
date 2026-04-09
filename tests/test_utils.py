from utils import compact_spaces, strip_html, short, md5_text, sha1_text


def test_compact_spaces():
    assert compact_spaces("  hello   world  ") == "hello world"
    assert compact_spaces("") == ""
    assert compact_spaces(None) == ""


def test_strip_html():
    assert strip_html("<b>bold</b> text") == "bold text"
    assert strip_html("&amp; &lt;") == "& <"
    assert strip_html("<script>alert(1)</script>ok") == "ok"


def test_short_within_limit():
    assert short("hello", 10) == "hello"


def test_short_truncates():
    result = short("hello world", 6)
    assert len(result) <= 6
    assert result.endswith("…")


def test_short_empty():
    assert short("", 5) == ""
    assert short(None, 5) == ""


def test_md5_text():
    h = md5_text("test")
    assert len(h) == 32
    assert h == md5_text("test")


def test_sha1_text():
    h = sha1_text("test")
    assert len(h) == 40
    assert h == sha1_text("test")
