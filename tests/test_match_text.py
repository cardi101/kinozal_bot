from match_text import similarity, normalize_match_text, text_tokens, token_overlap_ratio


def test_similarity_identical():
    assert similarity("Hello", "Hello") == 1.0


def test_similarity_empty():
    assert similarity("", "abc") == 0.0
    assert similarity(None, None) == 0.0


def test_similarity_partial():
    score = similarity("Breaking Bad", "Breaking")
    assert 0.5 < score < 1.0


def test_normalize_match_text_basic():
    result = normalize_match_text("  Hello   World  ")
    assert result == "hello world"


def test_normalize_match_text_html():
    result = normalize_match_text("<b>Test</b> &amp; More")
    assert "test" in result
    assert "more" in result


def test_normalize_match_text_translit():
    result = normalize_match_text("İstanbul Şehir")
    assert "istanbul" in result
    assert "sehir" in result


def test_normalize_match_text_cyrillic():
    result = normalize_match_text("Привет мир")
    assert result == "привет мир"


def test_text_tokens_basic():
    tokens = text_tokens("The Dark Knight")
    assert "dark" in tokens
    assert "knight" in tokens


def test_text_tokens_stopwords_filtered():
    tokens = text_tokens("The Lord of the Rings")
    assert "the" not in tokens
    assert "of" not in tokens
    assert "lord" in tokens
    assert "rings" in tokens


def test_text_tokens_empty():
    assert text_tokens("") == []
    assert text_tokens(None) == []


def test_token_overlap_ratio_identical():
    ratio = token_overlap_ratio("Breaking Bad", "Breaking Bad")
    assert ratio == 1.0


def test_token_overlap_ratio_no_overlap():
    ratio = token_overlap_ratio("Alpha Beta", "Gamma Delta")
    assert ratio == 0.0


def test_token_overlap_ratio_partial():
    ratio = token_overlap_ratio("Breaking Bad", "Breaking Good")
    assert 0.0 < ratio < 1.0
