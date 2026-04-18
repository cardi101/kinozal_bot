import re
from typing import Any, Dict, List, Optional, Tuple

from match_text import normalize_match_text, text_tokens, is_generic_cyrillic_title
from title_prep import (
    clean_release_title,
    looks_like_structured_numeric_title,
    looks_like_simple_numeric_title,
    is_bad_tmdb_candidate,
    extract_title_aliases_from_text,
    split_title_parts,
    is_release_group_candidate,
    extract_structured_numeric_title_candidates,
)
from utils import compact_spaces


ANIME_SEARCH_ALIAS_MAP: Dict[str, List[str]] = {
    "trigun": ["Trigun Stampede", "Trigun"],
    "триган": ["Trigun Stampede", "Trigun"],
    "gundam": ["Mobile Suit Gundam", "Gundam"],
    "гандам": ["Mobile Suit Gundam", "Gundam"],
    "detective conan": ["Detective Conan", "Case Closed"],
    "детектив конан": ["Detective Conan", "Case Closed"],
    "case closed": ["Detective Conan", "Case Closed"],
    "one piece": ["One Piece"],
    "ван пис": ["One Piece"],
    "naruto": ["Naruto"],
    "наруто": ["Naruto"],
    "bleach": ["Bleach"],
    "блич": ["Bleach"],
    "jojo": ["JoJo's Bizarre Adventure", "JoJo no Kimyou na Bouken"],
    "джоджо": ["JoJo's Bizarre Adventure", "JoJo no Kimyou na Bouken"],
    "evangelion": ["Neon Genesis Evangelion", "Evangelion"],
    "евангелион": ["Neon Genesis Evangelion", "Evangelion"],
    "gintama": ["Gintama"],
    "гинтама": ["Gintama"],
    "lupin": ["Lupin the Third", "Lupin III"],
    "люпен": ["Lupin the Third", "Lupin III"],
    "dragon ball": ["Dragon Ball"],
    "драгон болл": ["Dragon Ball"],
    "sailor moon": ["Sailor Moon"],
    "сейлор мун": ["Sailor Moon"],
    "monogatari": ["Monogatari"],
    "bakemonogatari": ["Monogatari"],
    "one punch man": ["One-Punch Man"],
    "ванпанчмен": ["One-Punch Man"],
    "mob psycho": ["Mob Psycho 100"],
    "soul land": ["Soul Land", "Douluo Continent"],
    "боевой континент": ["Soul Land", "Douluo Continent"],
    "douluo": ["Soul Land", "Douluo Continent"],
    "battle through the heavens": ["Battle Through the Heavens", "Fights Break Sphere"],
    "doupo cangqiong": ["Battle Through the Heavens", "Fights Break Sphere"],
    "the king s avatar": ["The King's Avatar", "Quan Zhi Gao Shou"],
    "аватар короля": ["The King's Avatar", "Quan Zhi Gao Shou"],
    "quan zhi gao shou": ["The King's Avatar", "Quan Zhi Gao Shou"],
    "link click": ["Link Click", "Shiguang Dailiren"],
    "агент времени": ["Link Click", "Shiguang Dailiren"],
    "shiguang dailiren": ["Link Click", "Shiguang Dailiren"],
    "scissor seven": ["Scissor Seven", "Cike Wu Liuqi"],
    "white cat legend": ["White Cat Legend", "Dali Si Rizhi"],
    "grandmaster of demonic cultivation": ["The Founder of Diabolism", "Mo Dao Zu Shi"],
    "mdzs": ["The Founder of Diabolism", "Mo Dao Zu Shi"],
    "mo dao zu shi": ["The Founder of Diabolism", "Mo Dao Zu Shi"],
    "heaven official s blessing": ["Heaven Official's Blessing", "Tian Guan Ci Fu"],
    "tian guan ci fu": ["Heaven Official's Blessing", "Tian Guan Ci Fu"],
}


ANIME_TITLE_MARKER_RE = re.compile(r"\b(?:ova|ona|anime|donghua|manhwa|manhua)\b", flags=re.I)


MANUAL_SEARCH_ALIAS_MAP: Dict[str, List[str]] = {
    "mihonhan saramdeului hyoyuljeokin mannam": [
        "Mihonnamnyeoui Hyoyuljeok Mannam",
        "The Practical Guide to Love",
        "Efficient Dating for Singles",
        "Efficient Dating of Single Men and Women",
    ],
    "mihonhan saramdeului hyoyuljeokin": [
        "Mihonnamnyeoui Hyoyuljeok Mannam",
        "The Practical Guide to Love",
        "Efficient Dating for Singles",
    ],
    "mihonhan saramdeului": [
        "Mihonnamnyeoui Hyoyuljeok Mannam",
        "The Practical Guide to Love",
        "Efficient Dating for Singles",
    ],
    "mihonnamnyeoui hyoyuljeok mannam": [
        "Mihonnamnyeoui Hyoyuljeok Mannam",
        "The Practical Guide to Love",
        "Efficient Dating for Singles",
    ],
    "미혼한 사람들의 효율적인 만남": [
        "미혼남녀의 효율적 만남",
        "Mihonnamnyeoui Hyoyuljeok Mannam",
        "The Practical Guide to Love",
        "Efficient Dating for Singles",
    ],
    "미혼남녀의 효율적 만남": [
        "미혼남녀의 효율적 만남",
        "Mihonnamnyeoui Hyoyuljeok Mannam",
        "The Practical Guide to Love",
        "Efficient Dating for Singles",
    ],
    "эффективные свидания для одиночек": [
        "Mihonnamnyeoui Hyoyuljeok Mannam",
        "The Practical Guide to Love",
        "Efficient Dating for Singles",
    ],
}


MANUAL_TMDB_OVERRIDE_MAP: Dict[str, Tuple[str, int]] = {
    "oshi no ko": ("tv", 203737),
    "звёздное дитя": ("tv", 203737),
    "ребёнок айдола": ("tv", 203737),
    "【推しの子】": ("tv", 203737),
}


def expand_tmdb_candidate_variants(text: str) -> List[str]:
    base = compact_spaces(text or "").strip(" /.-")
    if not base:
        return []

    variants: List[str] = []

    def add(value: str) -> None:
        value = compact_spaces(value or "").strip(" /.-")
        if not value or len(value.split()) > 8 or len(value) > 120:
            return
        if is_bad_tmdb_candidate(value):
            return
        if value not in variants:
            variants.append(value)

    add(base)
    add(clean_release_title(base))

    def should_skip_split_fragment(fragment: str, original: str) -> bool:
        fragment = compact_spaces(fragment or "").strip(" /.-")
        original = compact_spaces(original or "").strip(" /.-")
        if not fragment or not original:
            return False
        fragment_tokens = text_tokens(fragment)
        original_tokens = text_tokens(original)
        if len(fragment_tokens) == 1 and len(original_tokens) >= 2 and is_generic_cyrillic_title(fragment):
            return True
        return False

    def should_skip_short_prefix_colon_split(left: str, right: str, original: str) -> bool:
        left = compact_spaces(left or "").strip(" /.-")
        right = compact_spaces(right or "").strip(" /.-")
        original = compact_spaces(original or "").strip(" /.-")
        if not left or not right or not original:
            return False
        left_tail = compact_spaces(left.rsplit("/", 1)[-1]).strip(" /.-")
        left_tokens = text_tokens(left)
        left_tail_tokens = text_tokens(left_tail)
        right_tokens = text_tokens(right)
        if len(right_tokens) != 1:
            return False
        short_left_alias = len(left_tokens) == 1 and re.fullmatch(r"[A-Za-z0-9]{1,3}", left)
        short_left_tail_alias = len(left_tail_tokens) == 1 and re.fullmatch(r"[A-Za-z0-9]{1,3}", left_tail)
        if not short_left_alias and not short_left_tail_alias:
            return False
        # Re: Zero-like aliases should stay whole; splitting them into "Re" / "Zero"
        # produces noisy one-word queries and bad anime alias expansions.
        return True

    # Полезный алиас в круглых скобках должен идти отдельным кандидатом:
    # Wolgannamchin (Boyfriend on Demand) -> Boyfriend on Demand
    for match in re.finditer(r"\(([^()]{2,80})\)", base):
        inner = compact_spaces(match.group(1)).strip(" /.-")
        outer = compact_spaces(base[:match.start()] + " " + base[match.end():]).strip(" /.-")
        if inner and not is_bad_tmdb_candidate(inner):
            add(inner)
            add(clean_release_title(inner))
        if outer and outer != base:
            add(outer)
            add(clean_release_title(outer))
        if inner and outer:
            add(f"{outer} {inner}")

    for sep_pattern in [r"\s*:\s*", r"\s+[\-–—]\s+"]:
        parts = re.split(sep_pattern, base, maxsplit=1)
        if len(parts) == 2:
            left, right = parts[0], parts[1]
            if sep_pattern == r"\s*:\s*" and should_skip_short_prefix_colon_split(left, right, base):
                continue
            if not should_skip_split_fragment(left, base):
                add(left)
                add(clean_release_title(left))
            if not should_skip_split_fragment(right, base):
                add(right)
                add(clean_release_title(right))

    latin_words = re.findall(r"[A-Za-z][A-Za-z0-9'\-]*", base)
    if len(latin_words) >= 4:
        for n in (5, 4, 3, 2):
            if len(latin_words) >= n:
                add(" ".join(latin_words[:n]))
        if ":" in base:
            left = compact_spaces(base.split(":", 1)[0])
            add(left)
            left_words = re.findall(r"[A-Za-z][A-Za-z0-9'\-]*", left)
            if len(left_words) >= 2:
                add(" ".join(left_words[: min(4, len(left_words))]))

    return variants


def is_long_latin_tmdb_query(query: str) -> bool:
    query = compact_spaces(query or "")
    latin_words = re.findall(r"[A-Za-z][A-Za-z0-9'\-]*", query)
    if len(latin_words) < 4:
        return False
    return ":" in query or len(query) >= 28 or any("-" in word for word in latin_words)


def is_short_or_common_tmdb_query(query: str) -> bool:
    q = clean_release_title(query or "")
    tokens = text_tokens(q)
    latin_words = re.findall(r"[A-Za-z][A-Za-z0-9'\-]*", q)
    if len(tokens) <= 2:
        return True
    if len(q) <= 14:
        return True
    if len(latin_words) <= 2 and len(q) <= 18:
        return True
    return False


def is_short_acronym_tmdb_query(query: str) -> bool:
    q = compact_spaces(clean_release_title(query or ""))
    if not q:
        return False
    if not re.fullmatch(r"[A-Za-z]{2,4}", q):
        return False
    return q.upper() == q


def manual_tmdb_override_for_item(item: Dict[str, Any]) -> Optional[Tuple[str, int, str]]:
    kinozal_id = compact_spaces(str(item.get("kinozal_id") or ""))
    source_title = compact_spaces(str(item.get("source_title") or ""))
    cleaned_title = compact_spaces(str(item.get("cleaned_title") or ""))
    source_episode_progress = compact_spaces(str(item.get("source_episode_progress") or ""))

    source_blob = " | ".join(x for x in [source_title, cleaned_title, source_episode_progress] if x).lower()

    # Hotfix: Magalhães / Magellan (правильный TMDB movie/975335, 2025)
    # Ловим не только конкретный Kinozal ID, но и обе раздачи 720p/1080p по названию+году.
    if (
        kinozal_id == "2132322"
        or (
            "magalhães" in source_blob
            and "magellan" in source_blob
            and "2025" in source_blob
        )
    ):
        return "movie", 975335, "Magalhães"

    values = [
        source_title,
        cleaned_title,
        source_episode_progress,
    ]
    candidates: List[str] = []
    for value in values:
        if not value:
            continue
        candidates.append(value)
        candidates.extend(extract_title_aliases_from_text(value))
        candidates.extend(title_search_candidates(value, clean_release_title(value) or value))
    seen: set[str] = set()
    for value in candidates:
        norm = normalize_match_text(value)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        for key, override in MANUAL_TMDB_OVERRIDE_MAP.items():
            key_norm = normalize_match_text(key)
            if not key_norm:
                continue
            if norm == key_norm or key_norm in norm or norm in key_norm:
                media_type, tmdb_id = override
                return media_type, int(tmdb_id), key
    return None


def manual_alias_candidates_from_text(text: str) -> List[str]:
    base = compact_spaces(text or "").strip()
    if not base:
        return []
    norm = normalize_match_text(base)
    if not norm:
        return []
    norm_tokens = set(re.findall(r"[a-zа-яё0-9]+", norm))
    variants: List[str] = []

    def add(value: str) -> None:
        value = compact_spaces(value or "").strip(" /.-")
        if not value:
            return
        if looks_like_structured_numeric_title(value) or looks_like_simple_numeric_title(value):
            candidate = value
        else:
            candidate = clean_release_title(value) or value
        candidate = compact_spaces(candidate).strip(" /.-")
        if not candidate:
            return
        if not looks_like_structured_numeric_title(candidate) and not looks_like_simple_numeric_title(candidate) and is_bad_tmdb_candidate(candidate):
            return
        if candidate not in variants:
            variants.append(candidate)

    for key, aliases in MANUAL_SEARCH_ALIAS_MAP.items():
        key_norm = normalize_match_text(key)
        if not key_norm:
            continue
        key_tokens = set(re.findall(r"[a-zа-яё0-9]+", key_norm))
        if not key_tokens:
            continue
        matched = False
        if key_norm in norm or norm in key_norm:
            matched = True
        elif norm_tokens and key_tokens <= norm_tokens:
            matched = True
        elif norm_tokens and len(key_tokens) >= 2 and len(key_tokens & norm_tokens) >= max(2, len(key_tokens) - 1):
            matched = True
        if matched:
            for alias in aliases:
                add(alias)

    return variants


def anime_alias_candidates_from_text(text: str) -> List[str]:
    base = compact_spaces(text or "").strip()
    if not base:
        return []
    norm = normalize_match_text(base)
    if not norm:
        return []
    norm_tokens = set(re.findall(r"[a-zа-яё0-9]+", norm))
    variants: List[str] = []

    def add(value: str) -> None:
        value = compact_spaces(value or "").strip(" /.-")
        if not value or is_bad_tmdb_candidate(value):
            return
        if value not in variants:
            variants.append(value)

    for key, aliases in ANIME_SEARCH_ALIAS_MAP.items():
        key_norm = normalize_match_text(key)
        if not key_norm:
            continue
        key_tokens = set(re.findall(r"[a-zа-яё0-9]+", key_norm))
        if not key_tokens:
            continue
        matched = False
        if key_norm in norm or norm in key_norm:
            matched = True
        elif norm_tokens and key_tokens <= norm_tokens:
            matched = True
        elif norm_tokens and len(key_tokens) <= 3 and len(key_tokens & norm_tokens) >= len(key_tokens):
            matched = True
        if matched:
            for alias in aliases:
                add(alias)

    return variants


def title_search_candidates(source_title: str, cleaned_title: str) -> List[str]:
    ru, en = split_title_parts(source_title)
    candidates: List[str] = []
    numeric_titles = extract_structured_numeric_title_candidates(source_title) + extract_structured_numeric_title_candidates(cleaned_title)
    dedup_numeric_titles: List[str] = []
    for candidate in numeric_titles:
        candidate = compact_spaces(candidate)
        if candidate and candidate not in dedup_numeric_titles:
            dedup_numeric_titles.append(candidate)
    aliases = extract_title_aliases_from_text(source_title) + extract_title_aliases_from_text(cleaned_title)
    dedup_aliases: List[str] = []
    for alias in aliases:
        alias = compact_spaces(alias)
        if alias and alias not in dedup_aliases:
            dedup_aliases.append(alias)

    seeds = [
        *dedup_numeric_titles,
        en,
        ru,
        *dedup_aliases,
        clean_release_title(en),
        clean_release_title(ru),
        clean_release_title(cleaned_title or ""),
        clean_release_title(source_title or ""),
    ]

    def add_candidate(value: str) -> None:
        value = compact_spaces(value or "").strip()
        if not value:
            return
        if is_release_group_candidate(value):
            return
        if value not in candidates:
            candidates.append(value)

    for seed in seeds:
        for c in expand_tmdb_candidate_variants(seed):
            add_candidate(c)

    for seed in list(candidates) + seeds:
        for alias in manual_alias_candidates_from_text(seed or ""):
            for c in expand_tmdb_candidate_variants(alias):
                add_candidate(c)

    for seed in list(candidates) + seeds:
        for alias in anime_alias_candidates_from_text(seed or ""):
            for c in expand_tmdb_candidate_variants(alias):
                add_candidate(c)

    return candidates
