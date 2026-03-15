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


@dataclass
class AnimeMappingEntry:
    tmdb_id: int
    media_type: str
    source: str
    titles: List[str]
    year: Optional[int] = None
    raw: Optional[Dict[str, Any]] = None


class AnimeMappingStore:
    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)
        self.loaded = False
        self.entries: List[AnimeMappingEntry] = []
        self.by_title: Dict[str, List[AnimeMappingEntry]] = {}
        self.by_tmdb_id: Dict[int, AnimeMappingEntry] = {}

    def _iter_files(self) -> Iterable[Path]:
        if not self.base_dir.exists():
            return []
        return sorted(self.base_dir.rglob("*.json"))

    def _load_json(self, path: Path) -> Any:
        return json.loads(path.read_text(encoding="utf-8"))

    def _iter_records(self, payload: Any) -> Iterable[Dict[str, Any]]:
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    yield item
            return

        if isinstance(payload, dict):
            for key in ("data", "items", "anime", "entries", "mappings", "shows"):
                value = payload.get(key)
                if isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            yield item
                    return

            if payload:
                yield payload

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
            elif isinstance(value, dict):
                for item in value.values():
                    push(item)

        for key in ("title", "titles", "name", "names", "aliases", "synonyms"):
            push(record.get(key))

        for key in ("tmdb_title", "anilist_title", "mal_title", "original_title"):
            push(record.get(key))

        return result

    def _extract_tmdb(self, record: Dict[str, Any]) -> Optional[AnimeMappingEntry]:
        tmdb_movie_id = record.get("tmdb_movie_id")
        tmdb_show_id = record.get("tmdb_show_id")
        tmdb_id = record.get("tmdb_id") or record.get("themoviedb_id")

        media_type = str(record.get("media_type") or "").strip().lower()
        if tmdb_show_id not in (None, "", 0, "0"):
            try:
                return AnimeMappingEntry(
                    tmdb_id=int(tmdb_show_id),
                    media_type="tv",
                    source=str(record.get("source") or record.get("_source") or "unknown"),
                    titles=self._extract_titles(record),
                    year=self._extract_year(record),
                    raw=record,
                )
            except Exception:
                return None

        if tmdb_movie_id not in (None, "", 0, "0"):
            try:
                return AnimeMappingEntry(
                    tmdb_id=int(tmdb_movie_id),
                    media_type="movie",
                    source=str(record.get("source") or record.get("_source") or "unknown"),
                    titles=self._extract_titles(record),
                    year=self._extract_year(record),
                    raw=record,
                )
            except Exception:
                return None

        if tmdb_id not in (None, "", 0, "0"):
            try:
                guessed_media = media_type if media_type in {"tv", "movie"} else "tv"
                return AnimeMappingEntry(
                    tmdb_id=int(tmdb_id),
                    media_type=guessed_media,
                    source=str(record.get("source") or record.get("_source") or "unknown"),
                    titles=self._extract_titles(record),
                    year=self._extract_year(record),
                    raw=record,
                )
            except Exception:
                return None

        return None

    def _extract_year(self, record: Dict[str, Any]) -> Optional[int]:
        for key in ("year", "season_year", "release_year", "start_year"):
            value = record.get(key)
            try:
                if value is not None and str(value).strip():
                    return int(str(value).strip()[:4])
            except Exception:
                continue
        return None

    def load(self, force: bool = False) -> None:
        if self.loaded and not force:
            return

        self.entries = []
        self.by_title = {}
        self.by_tmdb_id = {}

        for path in self._iter_files():
            try:
                payload = self._load_json(path)
            except Exception:
                continue

            source_name = path.parent.name or path.stem

            for record in self._iter_records(payload):
                entry = self._extract_tmdb(record)
                if not entry:
                    continue
                if not entry.source or entry.source == "unknown":
                    entry.source = source_name

                self.entries.append(entry)
                self.by_tmdb_id[entry.tmdb_id] = entry

                for title in entry.titles:
                    norm = _normalize_title(title)
                    if not norm:
                        continue
                    self.by_title.setdefault(norm, []).append(entry)

        self.loaded = True

    def find_by_normalized_title(self, title_norm: str) -> List[AnimeMappingEntry]:
        if not self.loaded:
            self.load()
        return list(self.by_title.get(title_norm, []))

    def find_best(self, title_norms: List[str], year: Optional[int] = None) -> Optional[AnimeMappingEntry]:
        if not self.loaded:
            self.load()

        best: Optional[AnimeMappingEntry] = None
        best_score = -1

        seen_ids = set()
        candidates: List[AnimeMappingEntry] = []

        for norm in title_norms:
            for entry in self.by_title.get(norm, []):
                if entry.tmdb_id in seen_ids:
                    continue
                seen_ids.add(entry.tmdb_id)
                candidates.append(entry)

        for entry in candidates:
            score = 0

            normalized_titles = {_normalize_title(title) for title in entry.titles if _normalize_title(title)}
            if any(norm in normalized_titles for norm in title_norms):
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

            if score > best_score:
                best = entry
                best_score = score

        return best
