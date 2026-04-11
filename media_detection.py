import re

from utils import compact_spaces


_RUSSIAN_AUDIO_LABEL_RE = re.compile(r"(?<!\w)(?:ру|rus|russian)(?!\w)", flags=re.I)
_RUSSIAN_TITLE_BLOCK_RE = re.compile(
    r"(?:^|[ /,;|])(?:ру|rus|russian)(?:[ /,;|]|$)",
    flags=re.I,
)


def is_russian_release(item: dict | None) -> bool:
    item = item or {}
    category = compact_spaces(str(item.get("source_category_name") or ""))
    title = compact_spaces(str(item.get("source_title") or ""))
    if any(keyword in category for keyword in ("Русский", "Русская", "Русское", "Наше Кино")):
        return True
    if _RUSSIAN_TITLE_BLOCK_RE.search(title):
        return True

    for track in item.get("source_audio_tracks") or []:
        if _RUSSIAN_AUDIO_LABEL_RE.search(compact_spaces(str(track or ""))):
            return True
    return False


def is_non_video_release(text: str) -> bool:
    text = compact_spaces((text or "").lower())

    music_genres_pattern = r"(?:pop|rock|hip[\s\-]*hop|rap|jazz|blues|metal|disco|house|techno|trance|edm|folk|country|k[\s\-]*pop|j[\s\-]*pop|r[\s&]*b)"
    if re.search(rf"/\s*{music_genres_pattern}\s*/", text, flags=re.I):
        return True
    if re.search(rf"^[^/\n]{{2,120}}\s-\s[^/\n]{{2,120}}/\s*{music_genres_pattern}\s*/", text, flags=re.I):
        return True

    strong_patterns = [
        r"\bpc\s*\(windows\)\b",
        r"\bplaystation\b",
        r"\bps5\b",
        r"\bps4\b",
        r"\bxbox\b",
        r"\bnintendo\b",
        r"\bswitch\b",
        r"\brepack\b",
        r"\bdlc\b",
        r"\bgog\b",
        r"\bsteam\b",
        r"\bbuild\s*\d+\b",
        r"\bupdate\s*v?\d+(?:\.\d+)*\b",
        r"\bplugin\b",
        r"\bdriver\b",
        r"\bcrack\b",
        r"\bmp3\b",
        r"\bflac\b",
        r"\balac\b",
        r"\bape\b",
        r"\blossless\b",
        r"\bdiscography\b",
        r"\bvinyl\b",
        r"\bconcert\s+recording\b",
        r"\bаудиокнига\b",
        r"\bдискография\b",
        r"\bальбом\b",
        r"\bсингл\b",
        r"\bсаундтрек\b",
        r"\bтреклист\b",
        r"\bebook\b",
        r"\bpdf\b",
        r"\bepub\b",
        r"\bmobi\b",
        r"\bfb2\b",
        r"\bmagazine\b",
        r"\bжурнал\b",
        r"\bкомикс\b",
        r"\bманга\b",
        r"\bаудиоспектакль\b",
    ]
    if any(re.search(p, text, flags=re.I) for p in strong_patterns):
        return True

    software_platforms = [
        r"\bandroid\b", r"\bios\b", r"\bmacos\b", r"\blinux\b", r"\bportable\b",
        r"\bx64\b", r"\bx86\b", r"\bapk\b", r"\bipa\b", r"\bexe\b", r"\bmsi\b",
    ]
    software_context = [
        r"\bapp\b", r"\bapplication\b", r"\bsoftware\b", r"\bprogram\b", r"\butility\b",
        r"\binstaller\b", r"\bsetup\b", r"\bpatch\b", r"\bmod\b", r"\bplugin\b",
        r"\bdriver\b", r"\bbuild\s*\d+\b", r"\bupdate\s*v?\d+(?:\.\d+)*\b",
        r"\bверсия\b", r"\bустановщик\b", r"\bпрограмма\b",
    ]
    has_software_platform = any(re.search(p, text, flags=re.I) for p in software_platforms)
    has_software_context = any(re.search(p, text, flags=re.I) for p in software_context)
    if has_software_platform and has_software_context:
        return True

    game_platform_patterns = [
        r"\bpc\s*\(windows\)\b", r"\bwindows\b", r"\bx64\b", r"\bx86\b",
        r"\bportable\b", r"\brepack\b", r"\bgog\b", r"\bsteam\b", r"\bdlc\b",
    ]
    game_genre_patterns = [
        r"\brpg\b", r"\bsimulator\b", r"\bstrategy\b", r"\badventure\b",
        r"\baction\b", r"\bshooter\b", r"\bracing\b", r"\bhorror\b",
        r"\bsurvival\b", r"\barcade\b", r"\bquest\b",
    ]
    has_game_platform = any(re.search(p, text, flags=re.I) for p in game_platform_patterns)
    has_game_genre = any(re.search(p, text, flags=re.I) for p in game_genre_patterns)
    if has_game_platform and has_game_genre:
        return True

    bookish_patterns = [
        r"\bbook\b.+\b(?:pdf|epub|mobi|fb2)\b",
        r"\b(?:pdf|epub|mobi|fb2)\b.+\bbook\b",
        r"\bкнига\b.+\b(?:pdf|epub|mobi|fb2)\b",
        r"\b(?:pdf|epub|mobi|fb2)\b.+\bкнига\b",
    ]
    if any(re.search(p, text, flags=re.I) for p in bookish_patterns):
        return True

    sports_entities = [
        r"\bnhl\b", r"\bnba\b", r"\bufc\b", r"\bformula\s*1\b", r"\bformula\s*2\b",
        r"\bmoto\s*gp\b", r"\bmotogp\b", r"\bboxing\b", r"\bwrestling\b", r"\bfootball\b",
        r"\bsoccer\b", r"\btennis\b", r"\bbiathlon\b", r"\bski(?:ing)?\b", r"\brelay\b",
        r"\bхоккей\b", r"\bфутбол\b", r"\bтеннис\b", r"\bбиатлон\b", r"\bлыж\w*\b",
        r"\bбокс\b", r"\bмма\b", r"\bбаскетбол\b", r"\bформула\s*1\b", r"\bформула\s*2\b",
        r"\bмотогп\b", r"\bединоборств\w*\b", r"\bэстафет\w*\b",
        r"\bмасс[\s\-]?старт\b", r"\bспринт\w*\b", r"\bгонка\s+преследован\w*\b",
        r"\bиндивидуальн\w*\s+гонк\w*\b", r"\bпасьют\b",
    ]
    sports_context = [
        r"\bматч\b", r"\bчемпионат\b", r"\bтурнир\b", r"\blive\b", r"\bпрямая\s+трансляц\w*\b",
        r"\bэфир\b", r"\bобзор\b", r"\bраунд\b", r"\bvs\b", r"\bgrand\s+prix\b", r"\bgp\b",
        r"\bгонка\b", r"\bквалификац\w*\b", r"\bкубок\b", r"\bфинал\w*\b", r"\bлига\b",
        r"\bгран[\-\s]?при\b", r"\bэтап\b", r"\bспринт\w*\b", r"\bпрактик\w*\b",
        r"\brace\b", r"\bqualifying\b", r"\bsprint\b", r"\bpractice\b", r"\brelay\b",
        r"\bmass\s*start\b", r"\bpursuit\b", r"\bindividual\b", r"\bworld\s+cup\b",
        r"\bэстафет\w*\b", r"\bмасс[\s\-]?старт\b", r"\bгонка\s+преследован\w*\b",
        r"\bиндивидуальн\w*\s+гонк\w*\b", r"\bкубок\s+мира\b", r"\bчемпионат\s+мира\b",
        r"\b1/8\b", r"\b1/4\b", r"\b1/2\b", r"\bплей[\-\s]?офф\b", r"\bplay[\-\s]?off\b",
        r"\b[А-ЯA-Z][^/\n]{1,30}\s[-–]\s[А-ЯA-Z][^/\n]{1,30}\b",
    ]
    has_sports_entity = any(re.search(p, text, flags=re.I) for p in sports_entities)
    has_sports_context = any(re.search(p, text, flags=re.I) for p in sports_context)
    if has_sports_entity and has_sports_context:
        return True

    return False


def detect_media_type(text: str) -> str:
    text = (text or "").lower()
    if is_non_video_release(text):
        return "other"
    tv_patterns = [
        r"\bs\d{1,2}e\d{1,3}\b",
        r"\bseason\b",
        r"\bсер(ия|ии|ий)\b",
        r"\bсезон\b",
        r"\bэпизод\b",
        r"\bepisode\b",
    ]
    for pattern in tv_patterns:
        if re.search(pattern, text, flags=re.I):
            return "tv"
    return "movie"
