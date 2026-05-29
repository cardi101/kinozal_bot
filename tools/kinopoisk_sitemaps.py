#!/usr/bin/env python3
"""Standalone helper for downloading and parsing Kinopoisk sitemap files."""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Iterable

MAIN_SITEMAP_URL = "https://www.kinopoisk.ru/sitemaps/sitemap.xml"
DEFAULT_OUT_DIR = "kp_sitemaps_out"
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

FILM_URL_RE = re.compile(r"^https?://(?:www\.)?kinopoisk\.ru/film/(\d+)/?$", re.IGNORECASE)
SERIES_URL_RE = re.compile(r"^https?://(?:www\.)?kinopoisk\.ru/series/(\d+)/?$", re.IGNORECASE)
EPISODES_URL_RE = re.compile(
    r"^https?://(?:www\.)?kinopoisk\.ru/film/(\d+)/episodes/?$",
    re.IGNORECASE,
)
LOC_TEXT_RE = re.compile(rb"<(?:[A-Za-z0-9_.-]+:)?loc\b[^>]*>(.*?)</(?:[A-Za-z0-9_.-]+:)?loc>", re.DOTALL)


def log(message: str) -> None:
    print(message, file=sys.stderr)


def normalize_url(url: str) -> str:
    raw_url = (url or "").strip()
    if not raw_url:
        return ""

    if raw_url.startswith("//"):
        raw_url = "https:" + raw_url
    elif raw_url.startswith(("www.", "kinopoisk.ru/")):
        raw_url = "https://" + raw_url
    elif not urllib.parse.urlsplit(raw_url).scheme:
        raw_url = urllib.parse.urljoin(MAIN_SITEMAP_URL, raw_url)

    parts = urllib.parse.urlsplit(raw_url)
    if parts.scheme and parts.scheme not in {"http", "https"}:
        return raw_url

    scheme = parts.scheme or "https"
    netloc = parts.netloc
    path = urllib.parse.quote(urllib.parse.unquote(parts.path), safe="/:@")
    query = urllib.parse.quote(urllib.parse.unquote(parts.query), safe="=&?/:%+,-._~")
    return urllib.parse.urlunsplit((scheme, netloc, path, query, parts.fragment))


def fetch_bytes(url: str, retries: int = 3, timeout: int = 30) -> bytes:
    normalized_url = normalize_url(url)
    if not normalized_url:
        raise ValueError("empty URL")

    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        request = urllib.request.Request(
            normalized_url,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/xml,text/xml,application/gzip,*/*;q=0.8",
                "Accept-Encoding": "identity",
                "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
                "Cache-Control": "no-cache",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            last_error = error
            captcha_header = error.headers.get("x-yandex-captcha") if error.headers else None
            error_message = f"HTTP {error.code} {error.reason}"
            if captcha_header:
                error_message += f", x-yandex-captcha={captcha_header}"
            if attempt >= retries:
                break
            sleep_seconds = min(2 ** (attempt - 1), 10)
            log(f"[WARN] fetch failed ({attempt}/{retries}) for {normalized_url}: {error_message}; retry in {sleep_seconds}s")
            time.sleep(sleep_seconds)
        except (urllib.error.URLError, TimeoutError, OSError) as error:
            last_error = error
            if attempt >= retries:
                break
            sleep_seconds = min(2 ** (attempt - 1), 10)
            log(f"[WARN] fetch failed ({attempt}/{retries}) for {normalized_url}: {error}; retry in {sleep_seconds}s")
            time.sleep(sleep_seconds)

    raise RuntimeError(f"failed to fetch {normalized_url}: {last_error}")


def strip_namespace(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    return tag.rsplit(":", 1)[-1]


def text_of_child(element: ET.Element, child_name: str) -> str:
    for child in element:
        if strip_namespace(child.tag) == child_name and child.text:
            return child.text.strip()
    return ""


def filename_from_url(url: str) -> str:
    parts = urllib.parse.urlsplit(url)
    filename = Path(parts.path).name
    if not filename:
        filename = "sitemap.xml"
    return re.sub(r"[^A-Za-z0-9._-]+", "_", filename)


def make_unique_filename(filename: str, used: set[str]) -> str:
    if filename not in used:
        used.add(filename)
        return filename

    if filename.endswith(".xml.gz"):
        stem = filename[:-7]
        suffix = ".xml.gz"
    else:
        path = Path(filename)
        stem = path.stem or "sitemap"
        suffix = path.suffix

    counter = 2
    while True:
        candidate = f"{stem}-{counter}{suffix}"
        if candidate not in used:
            used.add(candidate)
            return candidate
        counter += 1


def classify_sitemap(filename: str) -> str:
    name = filename.lower()
    if "film" in name:
        return "film"
    if "series" in name or "serial" in name:
        return "series"
    if "trailer" in name or "video" in name:
        return "video_trailers"
    if "person" in name or "name" in name:
        return "persons"
    if "review" in name:
        return "reviews"
    if "news" in name:
        return "news"
    if "media" in name:
        return "media"
    if "list" in name:
        return "lists"
    return "other"


def extract_sitemap_entries(xml_bytes: bytes) -> list[dict[str, str]]:
    if xml_bytes.startswith(b"\x1f\x8b"):
        xml_bytes = gzip.decompress(xml_bytes)

    root = ET.fromstring(xml_bytes)
    used_filenames: set[str] = set()
    entries: list[dict[str, str]] = []

    for element in root.iter():
        if strip_namespace(element.tag) != "sitemap":
            continue

        url = normalize_url(text_of_child(element, "loc"))
        if not url:
            continue

        filename = make_unique_filename(filename_from_url(url), used_filenames)
        entries.append(
            {
                "url": url,
                "filename": filename,
                "lastmod": text_of_child(element, "lastmod"),
                "type": classify_sitemap(filename),
            }
        )

    if entries:
        return entries

    for element in root.iter():
        if strip_namespace(element.tag) != "loc" or not element.text:
            continue
        url = normalize_url(element.text)
        if not url:
            continue
        filename = make_unique_filename(filename_from_url(url), used_filenames)
        entries.append(
            {
                "url": url,
                "filename": filename,
                "lastmod": "",
                "type": classify_sitemap(filename),
            }
        )

    return entries


def download_sitemaps(
    entries: list[dict[str, str]],
    download_dir: Path,
    delay: float,
    retries: int = 3,
    timeout: int = 30,
) -> dict[str, int]:
    download_dir.mkdir(parents=True, exist_ok=True)
    stats = {"downloaded": 0, "skipped": 0, "errors": 0}

    for index, entry in enumerate(entries, start=1):
        target_path = download_dir / entry["filename"]
        if target_path.exists() and target_path.stat().st_size > 0:
            log(f"[SKIP] {index}/{len(entries)} {target_path.name}")
            stats["skipped"] += 1
            continue

        try:
            data = fetch_bytes(entry["url"], retries=retries, timeout=timeout)
            target_path.write_bytes(data)
            stats["downloaded"] += 1
            log(f"[OK] downloaded {index}/{len(entries)} {target_path.name} ({len(data)} bytes)")
        except Exception as error:
            stats["errors"] += 1
            log(f"[ERROR] download failed for {entry['url']}: {error}")

        if delay > 0 and index < len(entries):
            time.sleep(delay)

    return stats


def unpack_gz_files(download_dir: Path, unpacked_dir: Path) -> dict[str, int]:
    unpacked_dir.mkdir(parents=True, exist_ok=True)
    stats = {"unpacked": 0, "skipped": 0, "errors": 0}

    for source_path in sorted(download_dir.iterdir()):
        if not source_path.is_file():
            continue

        if source_path.name.endswith(".gz"):
            target_name = source_path.name[:-3]
        else:
            target_name = source_path.name
        target_path = unpacked_dir / target_name

        if target_path.exists() and target_path.stat().st_size > 0:
            stats["skipped"] += 1
            log(f"[SKIP] unpacked {target_path.name}")
            continue

        try:
            if source_path.name.endswith(".gz"):
                with gzip.open(source_path, "rb") as input_file:
                    data = input_file.read()
            else:
                data = source_path.read_bytes()
            target_path.write_bytes(data)
            stats["unpacked"] += 1
            log(f"[OK] unpacked {source_path.name} -> {target_path.name}")
        except Exception as error:
            stats["errors"] += 1
            log(f"[ERROR] unpack failed for {source_path}: {error}")

    return stats


def decode_xml_text(xml_bytes: bytes) -> str:
    try:
        return xml_bytes.decode("utf-8")
    except UnicodeDecodeError:
        return xml_bytes.decode("utf-8", errors="replace")


def extract_loc_values(xml_bytes: bytes) -> list[str]:
    if xml_bytes.startswith(b"\x1f\x8b"):
        xml_bytes = gzip.decompress(xml_bytes)

    try:
        root = ET.fromstring(xml_bytes)
        return [
            element.text.strip()
            for element in root.iter()
            if strip_namespace(element.tag) == "loc" and element.text and element.text.strip()
        ]
    except ET.ParseError:
        text = decode_xml_text(xml_bytes)
        loc_values: list[str] = []
        for match in LOC_TEXT_RE.finditer(text.encode("utf-8", errors="ignore")):
            value = match.group(1).decode("utf-8", errors="replace").strip()
            if value:
                loc_values.append(value)
        return loc_values


def canonical_title_url(url: str) -> tuple[str, str] | None:
    normalized_url = normalize_url(url)
    parts = urllib.parse.urlsplit(normalized_url)
    path = parts.path
    if not path.endswith("/"):
        path += "/"

    canonical = urllib.parse.urlunsplit((parts.scheme, parts.netloc.lower(), path, "", ""))
    if EPISODES_URL_RE.match(canonical):
        return "episodes", canonical
    if FILM_URL_RE.match(canonical):
        return "film", canonical
    if SERIES_URL_RE.match(canonical):
        return "series", canonical
    return None


def extract_kinopoisk_urls(unpacked_dir: Path) -> dict[str, set[str]]:
    result: dict[str, set[str]] = {
        "all": set(),
        "film": set(),
        "series": set(),
        "episodes": set(),
    }

    if not unpacked_dir.exists():
        log(f"[WARN] unpacked directory does not exist: {unpacked_dir}")
        return result

    for source_path in sorted(path for path in unpacked_dir.rglob("*") if path.is_file()):
        try:
            loc_values = extract_loc_values(source_path.read_bytes())
        except Exception as error:
            log(f"[ERROR] cannot read URLs from {source_path}: {error}")
            continue

        for loc_value in loc_values:
            classified = canonical_title_url(loc_value)
            if not classified:
                continue
            pattern_type, url = classified
            result[pattern_type].add(url)
            result["all"].add(url)

    return result


def extract_ids_from_urls(urls: Iterable[str], pattern_type: str) -> set[str]:
    ids: set[str] = set()
    patterns: list[re.Pattern[str]]
    if pattern_type == "film":
        patterns = [FILM_URL_RE]
    elif pattern_type == "series":
        patterns = [SERIES_URL_RE]
    elif pattern_type == "episodes":
        patterns = [EPISODES_URL_RE]
    else:
        patterns = [FILM_URL_RE, SERIES_URL_RE, EPISODES_URL_RE]

    for url in urls:
        canonical = canonical_title_url(url)
        candidate_url = canonical[1] if canonical else normalize_url(url)
        for pattern in patterns:
            match = pattern.match(candidate_url)
            if match:
                ids.add(match.group(1))
                break

    return ids


def sorted_ids(ids: Iterable[str]) -> list[str]:
    def sort_key(value: str) -> tuple[int, int | str]:
        if value.isdigit():
            return (0, int(value))
        return (1, value)

    return sorted(set(ids), key=sort_key)


def write_lines(path: Path, values: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(values) + "\n", encoding="utf-8")


def write_sitemap_outputs(out_dir: Path, entries: list[dict[str, str]]) -> None:
    write_lines(out_dir / "kinopoisk_sitemaps.txt", [entry["url"] for entry in entries])

    json_path = out_dir / "kinopoisk_sitemaps.json"
    json_path.write_text(json.dumps(entries, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    csv_path = out_dir / "kinopoisk_sitemaps.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=["type", "filename", "lastmod", "url"])
        writer.writeheader()
        for entry in entries:
            writer.writerow(
                {
                    "type": entry["type"],
                    "filename": entry["filename"],
                    "lastmod": entry["lastmod"],
                    "url": entry["url"],
                }
            )


def write_extracted_outputs(out_dir: Path, urls_by_type: dict[str, set[str]]) -> dict[str, set[str]]:
    ids_by_type = {
        "all": extract_ids_from_urls(urls_by_type["all"], "all"),
        "film": extract_ids_from_urls(urls_by_type["film"], "film"),
        "series": extract_ids_from_urls(urls_by_type["series"], "series"),
        "episodes": extract_ids_from_urls(urls_by_type["episodes"], "episodes"),
    }

    for pattern_type in ("all", "film", "series", "episodes"):
        write_lines(out_dir / f"kinopoisk_urls_{pattern_type}.txt", sorted(urls_by_type[pattern_type]))
        write_lines(out_dir / f"kinopoisk_ids_{pattern_type}.txt", sorted_ids(ids_by_type[pattern_type]))

    return ids_by_type


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download and parse Kinopoisk sitemap files.")
    parser.add_argument("--out", default=DEFAULT_OUT_DIR, help=f"output directory, default: {DEFAULT_OUT_DIR}")
    parser.add_argument("--download", action="store_true", help="download nested sitemap files")
    parser.add_argument("--extract-ids", action="store_true", help="unpack downloaded sitemaps and extract title URLs/IDs")
    parser.add_argument("--delay", type=float, default=0.7, help="delay between nested sitemap downloads, seconds")
    parser.add_argument("--retries", type=int, default=3, help="HTTP retry count")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout, seconds")
    return parser.parse_args(argv)


def print_summary(
    out_dir: Path,
    sitemap_count: int,
    download_stats: dict[str, int],
    unpack_stats: dict[str, int],
    urls_by_type: dict[str, set[str]],
    ids_by_type: dict[str, set[str]],
) -> None:
    print("")
    print("Summary:")
    print(f"  sitemap found: {sitemap_count}")
    print(f"  sitemap downloaded: {download_stats.get('downloaded', 0)}")
    print(f"  sitemap download skipped: {download_stats.get('skipped', 0)}")
    print(f"  sitemap download errors: {download_stats.get('errors', 0)}")
    print(f"  files unpacked: {unpack_stats.get('unpacked', 0)}")
    print(f"  unpack skipped: {unpack_stats.get('skipped', 0)}")
    print(f"  unpack errors: {unpack_stats.get('errors', 0)}")
    print(f"  URLs all/film/series/episodes: {len(urls_by_type['all'])}/{len(urls_by_type['film'])}/"
          f"{len(urls_by_type['series'])}/{len(urls_by_type['episodes'])}")
    print(f"  IDs all/film/series/episodes: {len(ids_by_type['all'])}/{len(ids_by_type['film'])}/"
          f"{len(ids_by_type['series'])}/{len(ids_by_type['episodes'])}")
    print(f"  output directory: {out_dir.resolve()}")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    sitemap_path = out_dir / "sitemap.xml"
    log(f"[INFO] fetching main sitemap: {MAIN_SITEMAP_URL}")
    try:
        main_sitemap = fetch_bytes(MAIN_SITEMAP_URL, retries=args.retries, timeout=args.timeout)
        sitemap_path.write_bytes(main_sitemap)
    except Exception as error:
        if sitemap_path.exists() and sitemap_path.stat().st_size > 0:
            log(f"[WARN] main sitemap fetch failed, using cached {sitemap_path}: {error}")
            main_sitemap = sitemap_path.read_bytes()
        else:
            log(f"[ERROR] main sitemap fetch failed: {error}")
            return 1

    try:
        entries = extract_sitemap_entries(main_sitemap)
    except Exception as error:
        log(f"[ERROR] cannot parse main sitemap: {error}")
        return 1

    write_sitemap_outputs(out_dir, entries)
    log(f"[INFO] found {len(entries)} nested sitemap entries")

    download_stats = {"downloaded": 0, "skipped": 0, "errors": 0}
    unpack_stats = {"unpacked": 0, "skipped": 0, "errors": 0}
    urls_by_type: dict[str, set[str]] = {"all": set(), "film": set(), "series": set(), "episodes": set()}
    ids_by_type: dict[str, set[str]] = {"all": set(), "film": set(), "series": set(), "episodes": set()}

    download_dir = out_dir / "downloaded_sitemaps"
    unpacked_dir = out_dir / "unpacked_sitemaps"

    if args.download:
        download_stats = download_sitemaps(
            entries,
            download_dir,
            delay=max(args.delay, 0),
            retries=max(args.retries, 1),
            timeout=max(args.timeout, 1),
        )

    if args.extract_ids:
        if args.download:
            unpack_stats = unpack_gz_files(download_dir, unpacked_dir)
        else:
            log("[WARN] --extract-ids was used without --download; reading existing unpacked_sitemaps directory")
        urls_by_type = extract_kinopoisk_urls(unpacked_dir)
        ids_by_type = write_extracted_outputs(out_dir, urls_by_type)

    print_summary(out_dir, len(entries), download_stats, unpack_stats, urls_by_type, ids_by_type)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
