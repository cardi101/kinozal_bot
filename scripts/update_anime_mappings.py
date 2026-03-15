import json
import shutil
import urllib.request
from pathlib import Path

BASE_DIR = Path("data/anime-mappings")
MANAMI_DIR = BASE_DIR / "manami"
KOMETA_DIR = BASE_DIR / "kometa"
MANAMI_DIR.mkdir(parents=True, exist_ok=True)
KOMETA_DIR.mkdir(parents=True, exist_ok=True)

MANAMI_RELEASE_API = "https://api.github.com/repos/manami-project/anime-offline-database/releases/latest"
MANAMI_TARGET_NAME = "anime-offline-database-minified.json"
MANAMI_TARGET_PATH = MANAMI_DIR / MANAMI_TARGET_NAME

KOMETA_RAW_URL = "https://raw.githubusercontent.com/Kometa-Team/Anime-IDs/master/anime_ids.json"
KOMETA_TARGET_PATH = KOMETA_DIR / "anime_ids.json"


def fetch_json(url: str) -> dict:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "kinozal-bot/1.0",
            "Accept": "application/vnd.github+json",
        },
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def download_file(url: str, target: Path) -> None:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "kinozal-bot/1.0"},
    )
    with urllib.request.urlopen(req, timeout=300) as resp, target.open("wb") as f:
        shutil.copyfileobj(resp, f)


def update_manami() -> None:
    release = fetch_json(MANAMI_RELEASE_API)
    assets = release.get("assets") or []
    asset = next((x for x in assets if x.get("name") == MANAMI_TARGET_NAME), None)
    if not asset:
        raise RuntimeError(f"Не найден asset {MANAMI_TARGET_NAME} в latest release manami")

    download_url = asset.get("browser_download_url")
    if not download_url:
        raise RuntimeError("У asset manami нет browser_download_url")

    download_file(download_url, MANAMI_TARGET_PATH)

    print("manami:")
    print(f"  release: {release.get('tag_name')}")
    print(f"  file:    {MANAMI_TARGET_PATH}")
    print(f"  size:    {MANAMI_TARGET_PATH.stat().st_size} bytes")


def update_kometa() -> None:
    download_file(KOMETA_RAW_URL, KOMETA_TARGET_PATH)
    print("kometa:")
    print(f"  file:    {KOMETA_TARGET_PATH}")
    print(f"  size:    {KOMETA_TARGET_PATH.stat().st_size} bytes")


def main() -> None:
    update_manami()
    update_kometa()


if __name__ == "__main__":
    main()

