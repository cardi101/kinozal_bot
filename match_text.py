import re
import unicodedata
from difflib import SequenceMatcher
from typing import List

from title_prep import clean_release_title
from utils import compact_spaces, strip_html


TITLE_STOPWORDS = {
    "the", "a", "an", "and", "of", "to", "in", "on", "for", "at", "by", "with",
    "la", "le", "el", "los", "las", "der", "die", "das", "de", "du",
    "и", "в", "во", "на", "по", "с", "со", "к", "ко", "от", "до", "из", "у", "про",
}


def similarity(a: str, b: str) -> float:
    a = (a or "").lower()
    b = (b or "").lower()
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def is_generic_cyrillic_title(value: str) -> bool:
    cleaned = clean_release_title(value or "")
    if not cleaned:
        return False
    if re.search(r"[A-Za-z]", cleaned):
        return False
    if not re.search(r"[А-Яа-яЁё]", cleaned):
        return False
    tokens = text_tokens(cleaned)
    return 1 <= len(tokens) <= 5


def normalize_match_text(text: str) -> str:
    text = compact_spaces(strip_html(text or "")).lower()
    if not text:
        return ""
    translit_map = str.maketrans({
        "ı": "i", "İ": "i", "ş": "s", "Ş": "s", "ğ": "g", "Ğ": "g",
        "ç": "c", "Ç": "c", "ö": "o", "Ö": "o", "ü": "u", "Ü": "u",
        "æ": "ae", "Æ": "ae", "œ": "oe", "Œ": "oe",
    })
    text = text.translate(translit_map)
    text = "".join(ch for ch in unicodedata.normalize("NFKD", text) if not unicodedata.combining(ch))
    text = re.sub(r"[^a-zа-яё0-9]+", " ", text, flags=re.I)
    return compact_spaces(text)


def text_tokens(text: str) -> List[str]:
    tokens = re.findall(r"[A-Za-zА-Яа-яЁё0-9]+", normalize_match_text(text or ""))
    filtered = [t for t in tokens if t not in TITLE_STOPWORDS]
    return filtered or tokens


def raw_text_tokens(text: str) -> List[str]:
    return re.findall(r"[A-Za-zА-Яа-яЁё0-9]+", normalize_match_text(text or ""))


def token_overlap_ratio(a: str, b: str) -> float:
    a_tokens = set(text_tokens(a))
    b_tokens = set(text_tokens(b))
    if not a_tokens or not b_tokens:
        return 0.0
    return len(a_tokens & b_tokens) / max(len(a_tokens), len(b_tokens), 1)
