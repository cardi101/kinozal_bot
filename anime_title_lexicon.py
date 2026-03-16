import json
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


def _compact(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _normalize_title(text: str) -> str:
    text = _compact(text).casefold()
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"[\\/|]+", " ", text)
    text = re.sub(r"[^\w\s]+", " ", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _extract_season_hint_from_text(text: str) -> Optional[int]:
    raw = _compact(text)
    if not raw:
        return None

    patterns = [
        r"\b(\d{1,2})\s*сезон\b",
        r"\bseason\s*(\d{1,2})\b",
        r"\b(\d{1,2})(?:st|nd|rd|th)\s+season\b",
        r"\bs(\d{1,2})\b",
    ]

    low = raw.casefold()
    for pattern in patterns:
        m = re.search(pattern, low, flags=re.I)
        if m:
            try:
                value = int(m.group(1))
                if value >= 1:
                    return value
            except Exception:
                pass
    return None


@dataclass
class AnimeLexiconEntry:
    canonical_title: str
    titles: List[str]
    media_type: str
    year: Optional[int] = None
    source: str = "manami"
    raw: Optional[Dict[str, Any]] = None
    season_hint: Optional[int] = None


class AnimeTitleLexicon:
    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)
        self.path = self.base_dir / "manami" / "anime-offline-database-minified.json"
        self.loaded = False
        self.entries: List[AnimeLexiconEntry] = []
        self.by_title: Dict[str, List[AnimeLexiconEntry]] = {}

    def _load_json(self) -> Any:
        return json.loads(self.path.read_text(encoding="utf-8"))

    def _iter_records(self, payload: Any) -> Iterable[Dict[str, Any]]:
        if isinstance(payload, dict) and isinstance(payload.get("data"), list):
            for item in payload["data"]:
                if isinstance(item, dict):
                    yield item

    def _extract_titles(self, record: Dict[str, Any]) -> List[str]:
        result: List[str] = []

        def push(value: Any) -> None:
            if isinstance(value, str):
                value = _compact(value)
                if value and value not in result:
                    result.append(value)
            elif isinstance(value, list):
                for item in value:
                    push(item)

        push(record.get("title"))
        push(record.get("synonyms"))
        return result

    def _extract_year(self, record: Dict[str, Any]) -> Optional[int]:
        anime_season = record.get("animeSeason")
        if isinstance(anime_season, dict):
            value = anime_season.get("year")
            try:
                if value is not None and str(value).strip():
                    return int(str(value).strip()[:4])
            except Exception:
                pass
        return None

    def _extract_media_type(self, record: Dict[str, Any]) -> str:
        raw_type = str(record.get("type") or "").strip().upper()
        if raw_type == "MOVIE":
            return "movie"
        return "tv"

    def _extract_entry_season_hint(self, titles: List[str]) -> Optional[int]:
        for title in titles:
            season = _extract_season_hint_from_text(title)
            if season is not None:
                return season
        return None

    def load(self, force: bool = False) -> None:
        if self.loaded and not force:
            return

        self.entries = []
        self.by_title = {}

        if not self.path.exists():
            self.loaded = True
            return

        payload = self._load_json()

        for record in self._iter_records(payload):
            titles = self._extract_titles(record)
            if not titles:
                continue

            entry = AnimeLexiconEntry(
                canonical_title=_compact(str(record.get("title") or "")),
                titles=titles,
                media_type=self._extract_media_type(record),
                year=self._extract_year(record),
                source="manami",
                raw=record,
                season_hint=self._extract_entry_season_hint(titles),
            )
            self.entries.append(entry)

            for title in titles:
                norm = _normalize_title(title)
                if not norm:
                    continue
                self.by_title.setdefault(norm, []).append(entry)

        self.loaded = True

    def find_by_normalized_title(self, title: str) -> List[AnimeLexiconEntry]:
        if not self.loaded:
            self.load()
        norm = _normalize_title(title)
        return list(self.by_title.get(norm, []))

    def find_best(self, title_candidates: List[str], year: Optional[int] = None) -> Optional[AnimeLexiconEntry]:
        if not self.loaded:
            self.load()

        normalized_queries: List[str] = []
        query_season_hint: Optional[int] = None

        for title in title_candidates:
            norm = _normalize_title(title)
            if norm and norm not in normalized_queries:
                normalized_queries.append(norm)
            if query_season_hint is None:
                query_season_hint = _extract_season_hint_from_text(title)

        best: Optional[AnimeLexiconEntry] = None
        best_score = -1
        seen = set()
        candidates: List[AnimeLexiconEntry] = []

        for norm in normalized_queries:
            for entry in self.by_title.get(norm, []):
                key = (entry.canonical_title, entry.year, entry.media_type, entry.season_hint)
                if key in seen:
                    continue
                seen.add(key)
                candidates.append(entry)

        for entry in candidates:
            score = 0
            entry_norms = {_normalize_title(t) for t in entry.titles if _normalize_title(t)}

            if any(q in entry_norms for q in normalized_queries):
                score += 100

            if year is not None and entry.year is not None:
                delta = abs(entry.year - year)
                if delta == 0:
                    score += 25
                elif delta == 1:
                    score += 15
                elif delta == 2:
                    score += 5

            if entry.media_type == "tv":
                score += 3

            if query_season_hint is not None:
                if entry.season_hint is None:
                    score += 5
                elif entry.season_hint == query_season_hint:
                    score += 60
                else:
                    score -= 80

            if score > best_score:
                best = entry
                best_score = score

        return best

