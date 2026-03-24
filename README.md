# Kinozal Bot

Telegram-бот для мониторинга новых релизов на [Kinozal.tv](https://kinozal.tv). Отправляет уведомления по подпискам с обогащением через TMDB — постеры, рейтинги, жанры, статус сериала.

![screenshot](./assets/screenshot.png)

![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![aiogram](https://img.shields.io/badge/aiogram-3.x-2CA5E0?logo=telegram&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-7-DC382D?logo=redis&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)

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
| `DATABASE_URL` | PostgreSQL DSN | ✅ |
| `REDIS_URL` | Redis URL | ✅ |
| `ALLOW_MODE` | `all` — открытый доступ, `invite` — только по инвайтам | — |
| `POLL_SECONDS` | Интервал опроса Kinozal (по умолчанию `120`) | — |
| `BOOTSTRAP_AS_READ` | При первом запуске пометить текущие релизы как доставленные (`1`/`0`) | — |
| `TMDB_LANGUAGE` | Язык TMDB (по умолчанию `ru-RU`) | — |
| `DEEP_LINK_BOT_USERNAME` | Username бота для магнет-ссылок | — |
| `MAGNET_BASE_URL` | Публичный URL магнет-сервера | — |

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
runtime_poller.py       — цикл поллинга и доставки (3 фазы)
runtime_app.py          — запуск бота и планировщика

kinozal_source.py       — парсинг ленты Kinozal.tv
kinozal_details.py      — детали раздачи (описание, файлы)
tmdb_client.py          — обогащение метаданными TMDB

db.py                   — операции с PostgreSQL
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

## Инфраструктура

| Сервис | Образ | Назначение |
|---|---|---|
| `app` | python:3.12-slim | Telegram-бот |
| `postgres` | postgres:16 | Основная БД |
| `redis` | redis:7-alpine | Кеш TMDB |
| `magnet-web` | python:3.12-slim | HTTP-редирект для магнет-ссылок |
| `caddy` | caddy:2 | Обратный прокси с TLS |

### Диагностика

```bash
# Логи
docker compose logs -f app

# Состояние сервисов
docker compose ps

# Консоль БД
docker compose exec postgres psql -U postgres -d kinozal_news

# Бэкап
docker compose exec -T postgres pg_dumpall -U postgres > backup.sql
```

---

## Лицензия

[MIT](./LICENSE)
