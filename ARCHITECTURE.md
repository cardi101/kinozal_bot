# Kinozal Bot — Architecture

## Назначение

Этот репозиторий содержит рефакторенную версию Kinozal Bot, вынесенную из личного исторического монолита в модульную структуру.

Цели рефакторинга:
- убрать перегруженный `app.py`
- разделить runtime, handlers, infra и helper/domain logic
- безопасно перенести рабочую БД со старого монолитного окружения
- зафиксировать воспроизводимое окружение для handoff и дальнейшей разработки

---

## Текущее состояние

На текущем этапе подтверждено следующее:

- рефакторенная версия успешно запускается на отдельном сервере
- БД от монолитной версии успешно перенесена и читается новой версией
- Telegram polling работает
- пользовательские и админские обработчики работают
- TMDB enrichment и загрузка постеров работают

---

## Структура окружения

### Контейнеры

Проект запускается через Docker Compose и использует следующие основные части:

- `app` — основной контейнер Telegram-бота
- `kinozal-postgres-refactor` — PostgreSQL для рефакторенной версии
- `kinozal-redis` — Redis для кеша / служебных задач

### Сетевая схема

Контейнер `app` работает с внешними сервисами через `host.docker.internal`:

- PostgreSQL:
  - `postgresql://postgres:postgres@host.docker.internal:5432/kinozal_news`
- Redis:
  - `redis://host.docker.internal:6379/0`
  - `http://host.docker.internal:8443`

---

## Entry point

### `app.py`

`app.py` — это composition root.

Он отвечает за:

- загрузку конфигурации
- создание зависимостей (`db`, `cache`, `tmdb`, `source`)
- регистрацию handlers
- запуск runtime

`app.py` **не должен** снова превращаться в:
- место для бизнес-логики
- место для работы с БД напрямую
- место для логики парсинга source
- место для крупных handler-реализаций

---

## Архитектурные слои

## 1. Infra layer

### `db.py`
Отвечает за:
- подключение к PostgreSQL
- запросы по пользователям, подпискам, релизам, доставке, meta
- служебные агрегаты и выборки для runtime/handlers

### `redis_cache.py`
Отвечает за:
- кеширование результатов TMDB
- negative cache
- уменьшение повторных сетевых запросов

### `tmdb_client.py`
Отвечает за:
- поиск и enrichment данных из TMDB
- работу с карточками фильмов/сериалов
- загрузку и использование постеров
- языковые и cache-настройки

Важно:
- base URL должен быть `https://api.themoviedb.org/3`
- URL постеров должен использовать `https://image.tmdb.org/...`

### `kinozal_source.py`
Отвечает за:
- работу с source feed
- нормализацию входных элементов
- подготовку релизов к дальнейшей обработке

---

## 2. Runtime layer

### `runtime_app.py`
Отвечает за:
- startup
- shutdown
- запуск и остановку фоновых задач
- корректную обработку `CancelledError`

### `runtime_poller.py`
Отвечает за:
- polling source
- orchestration цикла получения и обработки новых элементов
- взаимодействие между source, DB, TMDB и delivery

---

## 3. Handler layer

### Пользовательские сценарии
- `menu_views.py`
- `menu_handlers.py`
- `user_handlers.py`

### Подписки
- `subscription_basic_handlers.py`
- `subscription_filter_handlers.py`
- `subscription_input_handlers.py`
- `subscription_wizard_handlers.py`
- `subscription_test_handlers.py`

### Админские сценарии
- `admin_match_handlers.py`
- `admin_access_handlers.py`

### UI / keyboards
- `keyboards.py`

---

## 4. Domain / helper layer

### Базовые системные модули
- `config.py`
- `states.py`
- `utils.py`

### Парсинг и текст
- `parsing_basic.py`
- `parsing_audio.py`
- `title_prep.py`
- `match_text.py`
- `text_access.py`

### Категоризация и типизация
- `source_categories.py`
- `content_buckets.py`
- `media_detection.py`
- `country_helpers.py`
- `genres_helpers.py`
- `item_years.py`
- `keyword_filters.py`

### Версионирование релизов
- `release_versioning.py`

### TMDB matching
- `tmdb_aliases.py`
- `tmdb_match_validation.py`

### Подписки и совпадения
- `subscription_presets.py`
- `subscription_matching.py`
- `subscription_text.py`

### Доставка и служебная логика
- `delivery_formatting.py`
- `delivery_sender.py`
- `service_helpers.py`
- `source_health.py`
- `admin_helpers.py`

---

## Поток обработки нового релиза

Упрощённая схема:

1. `runtime_poller.py` обращается к source
2. source возвращает новые элементы
3. элемент проходит нормализацию и определение media type
4. если элемент video-like — запускается TMDB matching
5. `tmdb_client.py` возвращает match/enrichment
6. данные сохраняются в БД через `db.py`
7. вычисляются совпадения с подписками
8. `delivery_formatting.py` собирает сообщение
9. `delivery_sender.py` отправляет уведомление
10. служебные данные и dedupe-состояние фиксируются в БД

---

## Перенос БД от монолитной версии

### Источник
Старая монолитная версия использовала PostgreSQL с таблицами вида:

- `users`
- `subscriptions`
- `subscription_genres`
- `items`
- `item_genres`
- `deliveries`
- `meta`
- архивные таблицы

### Что подтверждено
Перенос дампа PostgreSQL от монолитной версии в рефакторенную версию прошёл успешно:
- структура БД читается
- обработчики работают
- тест подписок работает
- runtime и Telegram polling работают

### Важный вывод
Текущая модульная декомпозиция **совместима со старой схемой БД** без отдельного migration framework.

---

### Проблема
После переноса и рестартов было обнаружено:
- HTML страницы `Kinozal` приходит корректно
- но стандартный endpoint `/api/get/rss/kinozal` возвращает:
  - `{"Result":"Server is not available"}`

### Причина
- HTML приходит
- но стандартный селектор оказался слишком жёстким
- из-за этого массив `torrents` оставался пустым

### Решение
Для проекта добавлен локальный воспроизводимый ops-fix:

### Что делает patch
Patch добавляет fallback-логику для поиска строк релизов по ссылкам `details.php?id=...`, если основной селектор Kinozal не срабатывает.

### Как использовать
1. собрать локальный patched image:
2. поднять compose с override:

---

## Ключевые env-параметры

Наиболее важные переменные:

- `BOT_TOKEN`
- `ADMIN_IDS`
- `ALLOW_MODE`
- `TMDB_TOKEN`
- `DATABASE_URL`
- `REDIS_URL`
- `DISABLE_WEB_PAGE_PREVIEW`
- `BOOTSTRAP_AS_READ`
- `SOURCE_FETCH_LIMIT`
- `TMDB_CACHE_TTL`
- `TMDB_NEGATIVE_CACHE_TTL`
- `SOURCE_ERROR_ALERT_THRESHOLD`
- `SOURCE_ERROR_ALERT_REPEAT_MINUTES`
- `STARTUP_DB_RETRIES`
- `STARTUP_DB_RETRY_DELAY`
- `TMDB_LANGUAGE`

---

## Актуальные рабочие дефолты

- `BOOTSTRAP_AS_READ=1`
- `SOURCE_FETCH_LIMIT=50`
- `TMDB_CACHE_TTL=604800`
- `TMDB_NEGATIVE_CACHE_TTL=21600`
- `SOURCE_ERROR_ALERT_THRESHOLD=3`
- `SOURCE_ERROR_ALERT_REPEAT_MINUTES=180`

---

## Важные найденные грабли

## 1. Нельзя делать механическую замену `db.` -> `self.db.`
Это уже ломало:
- `api.themoviedb.org`
- `image.tmdb.org`

Любые массовые замены надо проверять вручную.

## 2. Проверки `psycopg` / PostgreSQL надо делать внутри контейнера `app`
Хостовый Python может не иметь нужных зависимостей.

Правильный шаблон:

```bash
docker compose exec -T app python - <<'PY'
# code
PY

Current deployment architecture
Runtime services

Проект использует следующие сервисы:

app — основной Telegram-бот

postgres — PostgreSQL 16 внутри docker compose

kinozal-redis — Redis для кеша / вспомогательных операций

Topology
Telegram -> app
app -> postgres:5432
app -> redis://kinozal-redis:6379/0
PostgreSQL

PostgreSQL больше не используется как отдельный вручную запущенный контейнер с пробросом порта на хост.

Актуальная схема:

сервис postgres объявлен в docker-compose.yml

app подключается к БД по внутреннему DNS-имени сервиса: postgres

внешний порт PostgreSQL на хост не публикуется

данные БД хранятся в docker volume, подключённом к сервису postgres

DATABASE_URL

Актуальный формат строки подключения:

DATABASE_URL=postgresql://postgres:change_this_password_please_2026@postgres:5432/kinozal_news
Security notes

PostgreSQL не должен быть доступен снаружи через 0.0.0.0:5432

доступ к БД должен идти только из внутренних контейнеров compose-сети

.env содержит секреты и не должен попадать в git

.env.example используется только как шаблон без реальных значений

Recovery notes

Для диагностики:

docker compose ps
docker compose logs --tail=100 app
docker compose logs --tail=100 postgres
docker compose exec postgres psql -U postgres -d kinozal_news

Для дампа:

docker compose exec -T postgres pg_dumpall -U postgres > backup.sql
Migration summary

Старая схема:

приложение подключалось к базе через host.docker.internal:5432

PostgreSQL жил вне compose как отдельный контейнер

порт БД был опубликован на хост

Новая схема:

PostgreSQL встроен в docker compose

приложение использует postgres:5432

внешний доступ к БД отключён

эксплуатация и запуск стали единообразными
