# Kinozal Bot

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

## Ops notes

- Для `TorAPI` и `Kinozal` используется локальный воспроизводимый patch: [ops/torapi/README.md](./ops/torapi/README.md)
- Compose override для patched `TorAPI`: [docker-compose.torapi-fixed.yml](./docker-compose.torapi-fixed.yml)

## Документация

- [Архитектура проекта](./ARCHITECTURE.md)
- [Снимок состояния handoff](./HANDOFF_STATUS.txt)


## Текущая схема запуска

Проект запускается через `docker compose` и состоит из трёх основных сервисов:

- `app` — Telegram-бот
- `postgres` — PostgreSQL 16
- `torapi` — HTTP API-источник для RSS / Kinozal

Redis используется отдельно как `kinozal-redis`.

## Сетевая схема

Теперь PostgreSQL работает **внутри compose-стека** и не публикуется наружу на хост.

Связи между сервисами:

- `app` -> `postgres:5432`
- `app` -> `host.docker.internal:8443` для `torapi`
- `app` -> `redis://kinozal-redis:6379/0`

## Переменные окружения

Основная строка подключения к БД:

```env
DATABASE_URL=postgresql://postgres:change_this_password_please_2026@postgres:5432/kinozal_news

Пример полного набора переменных — в .env.example.

Быстрый запуск
docker compose up -d --build
docker compose ps
docker compose logs -f app
Проверка PostgreSQL

Проверить доступ к базе можно так:

docker compose exec postgres psql -U postgres -d kinozal_news

Проверить состояние сервисов:

docker compose ps
docker compose logs --tail=100 app
docker compose logs --tail=100 postgres
Важно

PostgreSQL не должен быть опубликован наружу через ports

app должен использовать внутренний адрес postgres:5432, а не host.docker.internal:5432

.env не коммитится в репозиторий

для новых развёртываний использовать .env.example как шаблон

Резервное копирование

Создать дамп всех баз:

docker compose exec -T postgres pg_dumpall -U postgres > backup.sql

Восстановление:

cat backup.sql | docker compose exec -T postgres psql -U postgres -d postgres
Причина изменения схемы

Ранее приложение подключалось к PostgreSQL через host.docker.internal, а сама база жила вне compose-сервиса. После миграции PostgreSQL переведён внутрь compose-стека, чтобы:

убрать внешний доступ к БД

упростить сопровождение

сделать запуск предсказуемым

сократить зависимость от ручных контейнеров и нестабильных сетевых маршрутов
