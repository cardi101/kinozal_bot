import re
from typing import Any, Dict, Optional

from utils import compact_spaces


_MULTI_EPISODE_BLOCK_RE = r"\d+(?:\s*-\s*\d+)?(?:\s*,\s*\d+(?:\s*-\s*\d+)?)*"


def episode_progress_parts(value: Any) -> Optional[Dict[str, int | None]]:
    text = compact_spaces(str(value or "")).lower().replace("ё", "е")
    if not text:
        return None

    patterns = [
        rf"(?:(?P<season_start>\d+)\s*-\s*)?(?P<season>\d+)\s*сезон:\s*(?P<episodes>{_MULTI_EPISODE_BLOCK_RE})\s*(?:сер(?:ия|ии|ий)|выпуск(?:а|ов)?)\s*из\s*(?P<total>\d+)",
        rf"(?:(?P<season_start>\d+)\s*-\s*)?(?P<season>\d+)\s*сезон:\s*(?P<episodes>{_MULTI_EPISODE_BLOCK_RE})\s*(?:сер(?:ия|ии|ий)|выпуск(?:а|ов)?)",
        r"s(?P<season>\d{1,2})\s*e(?P<start>\d{1,3})\s*-\s*e(?P<end>\d{1,3})",
        r"s(?P<season>\d{1,2})\s*e(?P<start>\d{1,3})",
        r"(?P<season>\d{1,2})x(?P<start>\d{1,3})\s*-\s*(?:(?P=season)x)?(?P<end>\d{1,3})",
        r"(?P<season>\d{1,2})x(?P<start>\d{1,3})",
        rf"(?P<episodes>{_MULTI_EPISODE_BLOCK_RE})\s*(?:сер(?:ия|ии|ий)|выпуск(?:а|ов)?)\s*из\s*(?P<total>\d+)",
        rf"(?P<episodes>{_MULTI_EPISODE_BLOCK_RE})\s*(?:сер(?:ия|ии|ий)|выпуск(?:а|ов)?)",
        r"(?P<start>\d+)\s*-\s*(?P<end>\d+)\s*из\s*(?P<total>\d+)",
        r"(?P<start>\d+)\s*из\s*(?P<total>\d+)",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, flags=re.I)
        if not match:
            continue

        episodes_raw = match.groupdict().get("episodes")
        if episodes_raw:
            episode_numbers = [int(value) for value in re.findall(r"\d+", episodes_raw)]
            if not episode_numbers:
                continue
            start = episode_numbers[0]
            end = episode_numbers[-1]
        else:
            start_raw = match.groupdict().get("start")
            if start_raw is None:
                continue
            start = int(start_raw)
            end_raw = match.groupdict().get("end")
            end = int(end_raw) if end_raw is not None else start

        season_raw = match.groupdict().get("season")
        total_raw = match.groupdict().get("total")
        return {
            "season": int(season_raw) if season_raw is not None else None,
            "start": start,
            "end": end,
            "total": int(total_raw) if total_raw is not None else None,
        }
    return None


def parse_episode_progress(text: str) -> Optional[str]:
    text = compact_spaces(text or "")
    patterns = [
        rf"((?:\d+\s*-\s*)?\d+\s*сезон:\s*{_MULTI_EPISODE_BLOCK_RE}\s*сер(?:ия|ии|ий)\s*из\s*\d+)",
        rf"((?:\d+\s*-\s*)?\d+\s*сезон:\s*{_MULTI_EPISODE_BLOCK_RE}\s*выпуск(?:а|ов)?\s*из\s*\d+)",
        rf"((?:\d+\s*-\s*)?\d+\s*сезон:\s*{_MULTI_EPISODE_BLOCK_RE}\s*сер(?:ия|ии|ий))",
        rf"((?:\d+\s*-\s*)?\d+\s*сезон:\s*{_MULTI_EPISODE_BLOCK_RE}\s*выпуск(?:а|ов)?)",
        r"(\d+\s*-\s*\d+\s*сезон:\s*\d+\s*-\s*\d+\s*сер(?:ия|ии|ий)\s*из\s*\d+)",
        r"(\d+\s*-\s*\d+\s*сезон:\s*\d+\s*сер(?:ия|ии|ий)\s*из\s*\d+)",
        r"(\d+\s*-\s*\d+\s*сезон:\s*\d+\s*-\s*\d+\s*выпуск(?:а|ов)?\s*из\s*\d+)",
        r"(\d+\s*-\s*\d+\s*сезон:\s*\d+\s*выпуск(?:а|ов)?\s*из\s*\d+)",
        r"(\d+\s*сезон:\s*\d+\s*-\s*\d+\s*сер(?:ия|ии|ий)\s*из\s*\d+)",
        r"(\d+\s*сезон:\s*\d+\s*сер(?:ия|ии|ий)\s*из\s*\d+)",
        r"(\d+\s*сезон:\s*\d+\s*-\s*\d+\s*выпуск(?:а|ов)?\s*из\s*\d+)",
        r"(\d+\s*сезон:\s*\d+\s*выпуск(?:а|ов)?\s*из\s*\d+)",
        r"(\d+\s*-\s*\d+\s*сезон:\s*\d+\s*-\s*\d+\s*сер(?:ия|ии|ий))",
        r"(\d+\s*-\s*\d+\s*сезон:\s*\d+\s*сер(?:ия|ии|ий))",
        r"(\d+\s*-\s*\d+\s*сезон:\s*\d+\s*-\s*\d+\s*выпуск(?:а|ов)?)",
        r"(\d+\s*-\s*\d+\s*сезон:\s*\d+\s*выпуск(?:а|ов)?)",
        r"(\d+\s*-\s*\d+\s*сер(?:ия|ии|ий)\s*из\s*\d+)",
        r"(\d+\s*сер(?:ия|ии|ий)\s*из\s*\d+)",
        r"(\d+\s*-\s*\d+\s*выпуск(?:а|ов)?\s*из\s*\d+)",
        r"(\d+\s*выпуск(?:а|ов)?\s*из\s*\d+)",
        r"(\d+\s*сезон:\s*\d+\s*-\s*\d+\s*сер(?:ия|ии|ий))",
        r"(\d+\s*сезон:\s*\d+\s*сер(?:ия|ии|ий))",
        r"(\d+\s*сезон:\s*\d+\s*-\s*\d+\s*выпуск(?:а|ов)?)",
        r"(\d+\s*сезон:\s*\d+\s*выпуск(?:а|ов)?)",
        r"(\d+\s*-\s*\d+\s*сер(?:ия|ии|ий))",
        r"(\d+\s*сер(?:ия|ии|ий))",
        r"(\d+\s*-\s*\d+\s*выпуск(?:а|ов)?)",
        r"(\d+\s*выпуск(?:а|ов)?)",
        r"(s\d{1,2}\s*e\d{1,3}\s*-\s*e\d{1,3})",
        r"(s\d{1,2}\s*e\d{1,3})",
        r"(\d{1,2}x\d{1,3}\s*-\s*\d{1,2}x\d{1,3})",
        r"(\d{1,2}x\d{1,3}\s*-\s*\d{1,3})",
        r"(\d{1,2}x\d{1,3})",
        r"(\d+\s*-\s*\d+\s*из\s*\d+)",
        r"(\d+\s*из\s*\d+)",
    ]
    for pattern in patterns:
        m = re.search(pattern, text, flags=re.I)
        if m:
            return compact_spaces(m.group(1))
    return None
