import re
from datetime import datetime
from typing import List, Optional


def parse_year(text: str) -> Optional[int]:
    if not text:
        return None
    current = datetime.now().year + 1
    years = [int(x) for x in re.findall(r"(19\d{2}|20\d{2})", text)]
    years = [y for y in years if 1900 <= y <= current]
    return years[0] if years else None


def parse_years(text: str) -> List[int]:
    if not text:
        return []
    current = datetime.now().year + 1
    years = [int(x) for x in re.findall(r"(19\d{2}|20\d{2})", text)]
    years = [y for y in years if 1900 <= y <= current]
    uniq: List[int] = []
    for year in years:
        if year not in uniq:
            uniq.append(year)
    return uniq


def parse_format(text: str) -> Optional[str]:
    text = (text or "").lower()
    if "2160" in text or "4k" in text:
        return "2160"
    if "1080" in text:
        return "1080"
    if "720" in text:
        return "720"
    return None


def parse_imdb_id(text: str) -> Optional[str]:
    match = re.search(r"\b(tt\d{5,10})\b", text or "", flags=re.I)
    return match.group(1).lower() if match else None
