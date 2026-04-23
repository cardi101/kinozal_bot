import json
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

from episode_progress import episode_progress_parts, parse_episode_progress
from parsing_audio import infer_release_type, parse_audio_tracks
from parsing_basic import parse_format, parse_year
from title_prep import classify_release_segments, clean_release_title
from utils import compact_spaces


def _episode_progress_parts(value: Any) -> Dict[str, Optional[int]]:
    parts = episode_progress_parts(value)
    if not parts:
        return {
            "season": None,
            "episode_start": None,
            "episode_end": None,
            "episode_total": None,
        }
    return {
        "season": parts.get("season"),
        "episode_start": parts.get("start"),
        "episode_end": parts.get("end"),
        "episode_total": parts.get("total"),
    }


def _parse_episode_progress_text(value: Any) -> str:
    return compact_spaces(parse_episode_progress(value) or "")


@dataclass(slots=True)
class ParsedRelease:
    raw_title: str
    title_local: str = ""
    title_original: str = ""
    title_aliases: List[str] = field(default_factory=list)
    year: Optional[int] = None
    media_type: str = ""
    season: Optional[int] = None
    episode_start: Optional[int] = None
    episode_end: Optional[int] = None
    episode_total: Optional[int] = None
    episode_progress_text: str = ""
    release_type: str = ""
    resolution: str = ""
    codec: str = ""
    hdr: str = ""
    audio_tracks: List[str] = field(default_factory=list)
    subtitles: bool = False
    extra_tech_flags: List[str] = field(default_factory=list)
    classified_segments: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True)


def load_parsed_release(value: Any) -> Optional[ParsedRelease]:
    if isinstance(value, ParsedRelease):
        return value
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    try:
        return ParsedRelease(
            raw_title=compact_spaces(str(payload.get("raw_title") or "")),
            title_local=compact_spaces(str(payload.get("title_local") or "")),
            title_original=compact_spaces(str(payload.get("title_original") or "")),
            title_aliases=[
                compact_spaces(str(alias or ""))
                for alias in (payload.get("title_aliases") or [])
                if compact_spaces(str(alias or ""))
            ],
            year=int(payload["year"]) if payload.get("year") is not None else None,
            media_type=compact_spaces(str(payload.get("media_type") or "")),
            season=int(payload["season"]) if payload.get("season") is not None else None,
            episode_start=int(payload["episode_start"]) if payload.get("episode_start") is not None else None,
            episode_end=int(payload["episode_end"]) if payload.get("episode_end") is not None else None,
            episode_total=int(payload["episode_total"]) if payload.get("episode_total") is not None else None,
            episode_progress_text=compact_spaces(str(payload.get("episode_progress_text") or "")),
            release_type=compact_spaces(str(payload.get("release_type") or "")),
            resolution=compact_spaces(str(payload.get("resolution") or "")),
            codec=compact_spaces(str(payload.get("codec") or "")),
            hdr=compact_spaces(str(payload.get("hdr") or "")),
            audio_tracks=[
                compact_spaces(str(track or ""))
                for track in (payload.get("audio_tracks") or [])
                if compact_spaces(str(track or ""))
            ],
            subtitles=bool(payload.get("subtitles")),
            extra_tech_flags=[
                compact_spaces(str(flag or ""))
                for flag in (payload.get("extra_tech_flags") or [])
                if compact_spaces(str(flag or ""))
            ],
            classified_segments=list(payload.get("classified_segments") or []),
        )
    except Exception:
        return None


def coerce_parsed_release(raw_title: Any, media_type: Any = "", parsed_release_json: Any = "") -> ParsedRelease:
    normalized_raw_title = compact_spaces(str(raw_title or ""))
    normalized_media_type = compact_spaces(str(media_type or "")).lower()
    parsed = load_parsed_release(parsed_release_json)
    if parsed is not None:
        parsed_raw_title = compact_spaces(str(parsed.raw_title or ""))
        parsed_media_type = compact_spaces(str(parsed.media_type or "")).lower()
        if parsed_raw_title and normalized_raw_title and parsed_raw_title != normalized_raw_title:
            parsed = None
        elif parsed_media_type and normalized_media_type and parsed_media_type != normalized_media_type:
            parsed = None
    if parsed is not None:
        if not parsed.media_type and normalized_media_type:
            parsed.media_type = normalized_media_type
        if not parsed.raw_title:
            parsed.raw_title = normalized_raw_title
        return parsed
    return parse_release_title(raw_title, media_type)


def parse_release_title(raw_title: Any, media_type: Any = "") -> ParsedRelease:
    source_title = compact_spaces(str(raw_title or ""))
    normalized_media_type = compact_spaces(str(media_type or "")).lower()
    segments = classify_release_segments(source_title)
    title_segments = [segment for segment in segments if str(segment.get("kind") or "").startswith("title")]
    title_local = ""
    title_original = ""
    title_aliases: List[str] = []
    for segment in title_segments:
        title = compact_spaces(str(segment.get("title") or ""))
        if not title:
            continue
        if str(segment.get("kind") or "") == "title_ru" and not title_local:
            title_local = title
        if str(segment.get("kind") or "") == "title_en" and not title_original:
            title_original = title
        for alias in segment.get("aliases") or []:
            alias_clean = compact_spaces(str(alias or ""))
            if alias_clean and alias_clean not in title_aliases:
                title_aliases.append(alias_clean)

    if not title_local and title_segments:
        title_local = compact_spaces(str(title_segments[0].get("title") or ""))
    if not title_original and len(title_segments) >= 2:
        title_original = compact_spaces(str(title_segments[1].get("title") or ""))
    if not title_local:
        title_local = compact_spaces(clean_release_title(source_title))

    progress_text = _parse_episode_progress_text(source_title)
    progress = _episode_progress_parts(progress_text)
    lowered_title = source_title.lower()
    codec = "hevc" if any(token in lowered_title for token in ("hevc", "x265", "h265")) else "avc" if any(token in lowered_title for token in ("avc", "x264", "h264")) else ""
    hdr = "hdr" if "hdr" in lowered_title or "dolby vision" in lowered_title or "hdr10" in lowered_title else ""
    extra_tech_flags: List[str] = []
    for token in ("webrip", "web-dl", "bluray", "blu-ray", "remux", "hdtv", "sub"):
        if token in lowered_title and token not in extra_tech_flags:
            extra_tech_flags.append(token)

    audio_tracks = parse_audio_tracks(source_title)
    subtitles = any(track.upper() in {"СТ", "SUB"} for track in audio_tracks) or "subs" in lowered_title or " субт" in lowered_title
    return ParsedRelease(
        raw_title=source_title,
        title_local=title_local,
        title_original=title_original,
        title_aliases=title_aliases,
        year=parse_year(source_title),
        media_type=normalized_media_type,
        season=progress["season"],
        episode_start=progress["episode_start"],
        episode_end=progress["episode_end"],
        episode_total=progress["episode_total"],
        episode_progress_text=progress_text,
        release_type=infer_release_type(source_title) or "",
        resolution=parse_format(source_title) or "",
        codec=codec,
        hdr=hdr,
        audio_tracks=audio_tracks,
        subtitles=subtitles,
        extra_tech_flags=extra_tech_flags,
        classified_segments=segments,
    )
