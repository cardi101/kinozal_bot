# Kinopoisk Sitemaps

`tools/kinopoisk_sitemaps.py` is a standalone utility for manually working with the Kinopoisk sitemap index. It does not import or modify the bot runtime code and uses only the Python standard library.

## Commands

Collect the list of nested sitemap files:

```bash
python tools/kinopoisk_sitemaps.py
```

Download all nested sitemap files:

```bash
python tools/kinopoisk_sitemaps.py --download
```

Download, unpack gzip files, and extract Kinopoisk title URLs and IDs:

```bash
python tools/kinopoisk_sitemaps.py --download --extract-ids
```

Use a custom output directory:

```bash
python tools/kinopoisk_sitemaps.py --out data/kp_sitemaps --download --extract-ids
```

Useful network options:

```bash
python tools/kinopoisk_sitemaps.py --download --delay 1.0 --retries 5 --timeout 45
```

## Output

By default files are written to `kp_sitemaps_out/`:

```text
kp_sitemaps_out/
  sitemap.xml
  kinopoisk_sitemaps.txt
  kinopoisk_sitemaps.json
  kinopoisk_sitemaps.csv
  downloaded_sitemaps/
  unpacked_sitemaps/
  kinopoisk_urls_all.txt
  kinopoisk_urls_film.txt
  kinopoisk_urls_series.txt
  kinopoisk_urls_episodes.txt
  kinopoisk_ids_all.txt
  kinopoisk_ids_film.txt
  kinopoisk_ids_series.txt
  kinopoisk_ids_episodes.txt
```

The URL and ID files are created when `--extract-ids` is used. URL files are sorted lexicographically. ID files are deduplicated and sorted numerically where possible.

`kinopoisk_sitemaps.csv` has these columns:

```text
type,filename,lastmod,url
```

`type` is inferred from the sitemap filename, for example `film`, `series`, `video_trailers`, `persons`, `reviews`, `news`, `media`, `lists`, or `other`.

## Notes

The script sends a browser-like `User-Agent`, uses retries and timeouts, and skips already downloaded non-empty sitemap files. If Kinopoisk returns `403`, captcha, or other access restrictions from a server, the script logs the error and continues for nested sitemap downloads where possible.
