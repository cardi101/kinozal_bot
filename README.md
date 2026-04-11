# Kinozal Bot

[![CI](https://github.com/cardi101/kinozal_bot/actions/workflows/ci.yml/badge.svg)](https://github.com/cardi101/kinozal_bot/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/github/license/cardi101/kinozal_bot)](./LICENSE)
![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![aiogram](https://img.shields.io/badge/aiogram-3.x-2CA5E0?logo=telegram&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-7-DC382D?logo=redis&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)

Telegram-бот для мониторинга новых релизов на [Kinozal.tv](https://kinozal.tv). Отправляет уведомления по подпискам с обогащением через TMDB — постеры, рейтинги, жанры, статус сериала.

<p align="center">
  <img src="./assets/screenshot.png" alt="Kinozal Bot Screenshot" width="400" />
</p>

---

## Возможности

- **Гибкие подписки** — фильтры по типу медиа, году, рейтингу TMDB, жанрам, странам, форматам, ключевым словам
- **Пресеты и мастер** — готовые конфигурации (новинки кино, аниме, сериалы и др.) + пошаговое создание
- **TMDB-обогащение** — постер, рейтинг, обзор, статус сериала, дата следующей серии
- **Отслеживание изменений** — при изменении описания раздачи приходит обновление с выделенными строками (➕/➖)
- **Группировка** — несколько версий одного тайтла (разные озвучки, качество) объединяются в одно сообщение
- **Тихий режим** — настраиваемое окно тишины по UTC; уведомления накапливаются и доставляются после его окончания
- **Mute по названию** — кнопка 🔕 на каждом уведомлении, управление списком через меню
- **История доставок** — последние уведомления с датами и ссылками (`/history`)
- **Тест подписки** — предпросмотр совпадений на реальных данных без ожидания поллинга
- **Магнет-ссылки** — прямые ссылки через встроенный редирект-сервер
- **Инвайт-система** — доступ по приглашениям с ограничением числа использований и сроком
- **Админ-панель** — управление пользователями, доступами, диагностика совпадений TMDB

---

## Быстрый старт

### Требования

- Docker и Docker Compose
- Аккаунт на [Kinozal.tv](https://kinozal.tv)
- [Telegram Bot Token](https://t.me/BotFather)
- [TMDB API Read Token](https://www.themoviedb.org/settings/api)
- Redis — бот ожидает его по адресу из `REDIS_URL`. Если Redis не входит в твой стек, проще всего поднять его рядом:

```bash
docker run -d --name kinozal-redis --restart unless-stopped redis:7-alpine
```

### Установка

```bash
git clone https://github.com/cardi101/kinozal_bot.git
cd kinozal_bot

cp .env.example .env
nano .env

docker compose up -d --build
docker compose logs -f app
```

### Обновление

```bash
git pull
docker compose up -d --build
```

Миграции БД применяются автоматически при старте.
Для рабочего инстанса не используй `docker compose down -v`: этот флаг удаляет volume'ы, включая Postgres data.

### Локальная разработка

Для локальной проверки без Docker:

```bash
make install
make lint
make test
make check
make smoke
```

`make install` создаёт локальный `.venv` и ставит runtime-зависимости проекта вместе с `pytest` и `ruff`.
`make smoke` поднимает `postgres + redis + api`, проверяет `/health`, `schema_migrations` и bootstrap-пути `app`/`api` внутри контейнера.

---

## Конфигурация

Все настройки задаются через `.env`. Шаблон — `.env.example`.

| Переменная | Описание | Обязательно |
|---|---|---|
| `BOT_TOKEN` | Токен Telegram-бота | ✅ |
| `ADMIN_IDS` | Telegram user_id администраторов (через запятую) | ✅ |
| `KINOZAL_USERNAME` | Логин на Kinozal.tv | ✅ |
| `KINOZAL_PASSWORD` | Пароль на Kinozal.tv | ✅ |
| `TMDB_TOKEN` | TMDB API Read Access Token | ✅ |
| `POSTGRES_VOLUME_NAME` | Явное имя Docker volume для Postgres data, чтобы не получить новый пустой volume при смене compose project name | — |
| `DATABASE_URL` | PostgreSQL DSN | ✅ |
| `REDIS_URL` | Redis URL | ✅ |
| `ALLOW_MODE` | `open` — открытый доступ, `invite` — только по инвайтам, `manual` — ручная выдача доступа | — |
| `POLL_SECONDS` | Интервал опроса Kinozal (по умолчанию `120`) | — |
| `BOOTSTRAP_AS_READ` | При первом запуске пометить текущие релизы как доставленные (`1`/`0`) | — |
| `TMDB_LANGUAGE` | Язык TMDB (по умолчанию `ru-RU`) | — |
| `DEEP_LINK_BOT_USERNAME` | Username бота для магнет-ссылок | — |
| `MAGNET_BASE_URL` | Публичный URL магнет-сервера, например `https://magnet.example.com` | — |
| `API_HOST` | Host для optional HTTP API (по умолчанию `0.0.0.0`) | — |
| `API_PORT` | Порт для optional HTTP API (по умолчанию `8000`) | — |
| `ADMIN_HTTP_TOKEN` | Токен для `/admin/*` HTTP endpoints; если пустой, admin HTTP выключен | — |

> **Магнет-ссылки** требуют публичного домена с HTTPS — Telegram не открывает `http://` ссылки в клиенте.
> Нужно: купить домен, направить его A-запись на сервер, прописать в `Caddyfile` и задать `MAGNET_BASE_URL`.
> Если магнет-ссылки не нужны — просто не задавай эти переменные, бот работает без них.

---

## Команды

| Команда | Описание |
|---|---|
| `/menu` | Главное меню |
| `/subs` | Список подписок |
| `/history` | Последние 15 доставленных релизов |
| `/muted` | Заглушённые названия |
| `/quiet [ЧЧ ЧЧ\|off]` | Тихий режим: установить окно или отключить |
| `/start` | Начало работы, активация инвайта |

### Управление доступом

При `ALLOW_MODE=invite` пользователи активируют бота через инвайт-код. Создать инвайт можно из админ-панели:

```
/create_invite <uses> <days> <note>
```

Пример: `/create_invite 1 30 друг` — одноразовый инвайт на 30 дней.
Пользователь вводит код через `/start КОД` или просто отправляет код боту.

---

## Архитектура

```
app.py                  — точка входа, регистрация роутеров
app_bootstrap.py        — composition root, сборка зависимостей и router wiring
runtime_poller.py       — тонкий worker entrypoint, сборка зависимостей
runtime_app.py          — запуск бота и планировщика
api.py                  — ASGI entrypoint для optional HTTP API
api_app.py              — FastAPI routes и auth dependency
api_bootstrap.py        — composition root для HTTP API

services/worker_service.py      — orchestration цикла поллинга и доставки
services/kinozal_service.py     — фасад над Kinozal source/details
services/tmdb_service.py        — фасад над TMDB client
services/subscription_service.py — матчинг и работа с подписками
services/delivery_service.py    — доставка и группировка уведомлений
services/admin_api_service.py   — health/metrics/admin debug/reparse facade

domain/models.py        — внутренние модели item/subscription/delivery для worker pipeline

repositories/worker_repository.py — repository adapter для worker-цикла
repositories/users_repository.py  — пользователи, инвайты, quiet-hours
repositories/subscriptions_repository.py — подписки, жанры, страны
repositories/items_repository.py  — items, timelines, cleanup, rematch
repositories/delivery_repository.py — deliveries, debounce, muted, history
repositories/meta_repository.py   — meta и TMDB genres
db_migrations.py        — migration runner и schema_migrations bookkeeping
migrations/0001_initial_schema.sql — baseline schema migration
db.py                   — тонкий PostgreSQL facade и запуск migrations
redis_cache.py          — кеш TMDB-запросов

subscription_matching.py   — матчинг элемента под подписку
delivery_formatting.py     — форматирование сообщений
delivery_sender.py         — отправка в Telegram

*_handlers.py           — обработчики команд и кнопок
keyboards.py            — inline-клавиатуры
```

**Цикл доставки (3 фазы):**

1. **Сбор** — новые элементы обогащаются через TMDB, проверяются изменения текста релиза
2. **Flush** — доставка накопленных уведомлений после окончания тихого режима
3. **Доставка** — матчинг по подпискам, группировка по TMDB ID, учёт тихого режима

---

## HTTP API

`FastAPI` здесь опционален и не заменяет worker. Основной engine остаётся в poller-процессе, а HTTP-слой нужен для health, diagnostics и admin actions.

Доступные endpoints:

- `GET /health`
- `GET /metrics`
- `GET /admin/subscriptions/{user_id}`
- `GET /admin/match-debug?kinozal_id=...&live=true`
- `POST /admin/reparse/{kinozal_id}`

`/admin/*` endpoints защищены заголовком `X-Admin-Token`. Если `ADMIN_HTTP_TOKEN` не задан, admin HTTP endpoints отключены и возвращают `503`.

Примеры:

```bash
curl http://localhost:8000/health
curl http://localhost:8000/metrics
curl -H "X-Admin-Token: $ADMIN_HTTP_TOKEN" http://localhost:8000/admin/subscriptions/123456789
curl -H "X-Admin-Token: $ADMIN_HTTP_TOKEN" "http://localhost:8000/admin/match-debug?kinozal_id=12345&live=true"
curl -X POST -H "X-Admin-Token: $ADMIN_HTTP_TOKEN" http://localhost:8000/admin/reparse/12345
```

---

## Инфраструктура

| Сервис | Образ | Назначение |
|---|---|---|
| `app` | python:3.12-slim | Telegram-бот |
| `api` | python:3.12-slim | Optional FastAPI facade для health/admin/debug |
| `postgres` | postgres:16 | Основная БД |
| `redis` | redis:7-alpine | Кеш TMDB |
| `magnet-web` | python:3.12-slim | HTTP-редирект для магнет-ссылок |
| `caddy` | caddy:2 | Обратный прокси с TLS |

### Диагностика

```bash
# Логи
docker compose logs -f app
docker compose logs -f api

# Состояние сервисов
docker compose ps

# Консоль БД
docker compose exec postgres psql -U postgres -d kinozal_news

# Бэкап
docker compose exec -T postgres pg_dumpall -U postgres > backup.sql

# Активный volume с данными Postgres
docker volume inspect "$POSTGRES_VOLUME_NAME"
```

Postgres data привязан к явному имени volume из `POSTGRES_VOLUME_NAME`. Это снижает риск потерять БД из-за смены compose project name или другого окружения с тем же репозиторием.

---

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Make your changes and ensure `make check` passes
4. Commit (`git commit -m 'Add my feature'`)
5. Push and open a Pull Request

---

## Фильтрация русского контента

По умолчанию бот **пропускает раздачи с русским контентом** — категории «Русский», «Русская», «Русское», «Наше Кино» и тайтлы с меткой `/ РУ /`.

Чтобы отключить этот фильтр, удалите в `services/worker_service.py` блок:

```python
if any(kw in category for kw in ("Русский", "Русская", "Русское", "Наше Кино")) or "/ РУ /" in title:
    log.info("Skip Russian item: %s [%s]", title, category)
    continue
```

После этого пересоберите контейнер: `docker compose up -d --build`.

---

## Лицензия

[MIT](./LICENSE)
