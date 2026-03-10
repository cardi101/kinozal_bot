# Kinozal News Bot Rebuild v2

## Что исправлено
- BOOTSTRAP_AS_READ по умолчанию выключен
- SOURCE_FETCH_LIMIT по умолчанию 200
- TMDB не дергается для игр/софта и прочего не-video контента
- разные WEB-DL / WEB-DLRip / BDRip различаются в сообщении
- апдейты по сериям отображаются в сообщении
- dedupe опирается на source_uid + cleaned_title + release_type + resolution + episode_progress + year
- bootstrap не делает TMDB enrichment, если включен режим mark-as-read
- docker-compose уже с DNS для app

## Как снести старый стек
```bash
docker compose down -v --remove-orphans
```
Если лежало в старой папке, потом можно удалить папку проекта: `rm -rf /opt/ИМЯ_ПАПКИ`

## Запуск
```bash
cp .env.example .env
nano .env
docker compose up -d --build
docker compose logs -f app
```
