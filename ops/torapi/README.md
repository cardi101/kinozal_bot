# TorAPI Kinozal fix

## Что это

Локальный патч для `TorAPI`, который добавляет fallback-селектор для Kinozal RSS custom parser.

Проблема:
- `TorAPI` успешно получает HTML `browse.php` от `kinozal.tv`
- но `/api/get/rss/kinozal` возвращает `{"Result":"Server is not available"}`
- причина — слишком жёсткий парсинг таблицы Kinozal

## Что лежит рядом

- `torapi-kinozal-fix.patch` — diff-патч для upstream `TorAPI`
- `build-local-torapi-fixed.sh` — скрипт, который:
  - клонирует upstream `TorAPI`
  - применяет patch
  - собирает локальный Docker image `torapi:kinozal-fix`

## Как собрать локальный образ

```bash
cd /opt/kinozal_bot
./ops/torapi/build-local-torapi-fixed.sh
