# База данных: куда бот складывает всё из Telegram

Бот «Око» (Oko Telegram Bot) – это не только процесс, который читает Telegram, но и база, в которой лежит результат. Бот сам базу не создаёт: ему нужен PostgreSQL, к которому он подключается по строке `ARCHIVE_DATABASE_DSN`. Таблицы бот создаёт сам командой `migrate` (файлы `migrations/*.sql`). Файлы вложений (голосовые, документы, видео) в базу не попадают, они лежат на диске в каталоге `media` (см. раздел 3).

Все SQL-запросы и команды ниже, кроме отдельно отмеченных, выполнены на тестовой базе PostgreSQL 16.14, созданной через `migrate` и заполненной синтетическими сообщениями через код бота (список – в конце документа). Docker в среде проверки был недоступен, поэтому команды `docker compose` не запускались.

## Выбор: маленький проект или большой

**Коротко.**

- **Маленький проект** – один владелец, несколько чатов, небольшой VPS. Берите один контейнер PostgreSQL из `compose.yaml` с экономными настройками памяти. Дополнительных сервисов нет.
- **Большой проект** – много чатов, команда, аналитика, несколько агентов, которые читают данные. Берите свой Supabase (облачный проект или self-hosted) как PostgreSQL.

### Что умеет этот код, а что нет

Код работает **только с PostgreSQL**. Драйвер – psycopg 3, SQL написан под PostgreSQL: сеансовые блокировки `pg_try_advisory_lock`, очередь задач на `FOR UPDATE SKIP LOCKED`, `jsonb`, `bytea`, `GENERATED ALWAYS AS IDENTITY`, `timestamptz`, массивы в параметрах (`= ANY(%s)`). SQLite и другие встроенные базы **не поддерживаются и не проверялись**; «компактная база» здесь означает один небольшой PostgreSQL, а не файловую базу.

*Идея, не реализована и не планируется в этом репозитории: что потребовалось бы для SQLite.* Заменить слой подключения (`archive/db.py`) и все модули, которые пишут SQL напрямую (`ingest`, `approvals`, `worker`, `notifications`, `transcription`, `transcripts`, `api`, `export`, `status`); вместо блокировок PostgreSQL сделать один процесс-писатель на файловой блокировке; переписать выбор задач без `SKIP LOCKED`; заменить `jsonb`-операции в очистке очереди и в сводке неизвестных чатов, `make_interval`, `date_trunc ... AT TIME ZONE`, `pg_stat_activity` в сверке записи файла; переписать миграции; перенести набор тестов, который сейчас идёт на настоящем PostgreSQL. Это существенная переделка, а не настройка.

### Маленький проект: компактный PostgreSQL

Что получаете: сервис `db` в `compose.yaml` – единственный дополнительный контейнер рядом с `api`, `poller`, `worker`. Ни Redis, ни брокера, ни объектного хранилища нет. Порт базы наружу не публикуется.

В `compose.yaml` сервис `db` стартует с такими параметрами (значения по умолчанию; их можно менять в `.env`, см. `.env.example`) и ограничением памяти контейнера:

| Параметр | По умолчанию | Переменная в `.env` | Как выбирать |
|---|---|---|---|
| `shared_buffers` | 64MB | `PG_SHARED_BUFFERS` | Общий кэш страниц. В документации PostgreSQL ориентир – около четверти памяти, отведённой базе; на маленьком сервере берите меньше и следите, чтобы он помещался в лимит контейнера. |
| `work_mem` | 4MB | `PG_WORK_MEM` | Память на одну операцию сортировки или хеша у одного соединения. Худший случай: число одновременных соединений × `work_mem` × число операций в запросе. Не повышайте, пока нет медленных сортировок. |
| `maintenance_work_mem` | 32MB | `PG_MAINTENANCE_WORK_MEM` | Память для `VACUUM` и создания индексов. |
| `effective_cache_size` | 192MB | `PG_EFFECTIVE_CACHE_SIZE` | Подсказка планировщику, сколько всего кэшируется (память под `shared_buffers` плюс кэш ОС); память не выделяет. Ставьте порядка лимита контейнера минус память под сами процессы. |
| `max_connections` | 20 | `PG_MAX_CONNECTIONS` | Правило ниже. |
| `max_parallel_workers_per_gather` | 0 (зашито) | – | Параллельные запросы отключены: меньше пиков памяти, хватает для чата и выгрузки. |
| `autovacuum_max_workers` | 2 (зашито) | – | Меньше одновременных процессов очистки. |
| лимит памяти контейнера | 384m | `DB_MEM_LIMIT` | Правило ниже. |

**Правило для `max_connections`.** По коду поллер держит 1 соединение; воркер – основное и ещё одно на каждую выполняемую задачу (продление аренды), изредка третье для сверки; API на каждый запрос открывает соединение для чтения и отдельное для записи в `read_audit`; ещё нужны `psql` и разовые команды (`ops`). Итого `max_connections` ≥ 7 + 2 × (наибольшее число одновременных запросов к API) с запасом. Если одновременных читателей больше, чем помещается в это значение, увеличьте `PG_MAX_CONNECTIONS` и проверьте лимит памяти.

**Правило для лимита памяти контейнера.** `DB_MEM_LIMIT` ≥ `shared_buffers` + `max_connections` × (расход одного процесса PostgreSQL; несколько мегабайт, плюс `work_mem` у активных) + `maintenance_work_mem` + запас на кэш файловой системы. Если уменьшаете `shared_buffers`, лимит можно снижать вслед за ним.

**Значения – отправная точка, а не гарантия.** Они подобраны так, чтобы PostgreSQL запускался, принимал миграции и обслуживал бота и API; на тестовой базе PostgreSQL 16 проверено, что сервер стартует с этими параметрами, `SHOW` показывает заданные значения, `migrate`, загрузка тестовых сообщений и 8 параллельных запросов к API отрабатывают без ошибок. Реальное потребление памяти на Linux в контейнере автор не измерял. Проверьте на своём сервере под реальной нагрузкой:

```sh
docker stats --no-stream                                      # потребление памяти контейнерами
docker inspect -f '{{.State.OOMKilled}}' "$(docker compose ps -q db)"   # true – контейнер убит за превышение лимита
docker compose exec db psql -U archive -d archive -c 'SHOW shared_buffers' -c 'SHOW max_connections'
```

Если контейнер убивают по памяти – поднимите `DB_MEM_LIMIT` (и при необходимости уменьшите `PG_MAX_CONNECTIONS`). Изменение применяется при пересоздании контейнера: `docker compose up -d --force-recreate db`; данные при этом сохраняются, так как это параметры запуска сервера, а не инициализации.

Лимиты других контейнеров (`poller` 256m, `api` 512m, `worker` 1g) – потолки, а не резерв. Сумму потолков сравните с оперативной памятью сервера и проверьте фактический расход через `docker stats`.

### Большой проект: свой Supabase

Supabase – это PostgreSQL с набором надстроек. Бот использует из него **только PostgreSQL**; остальное (панель, REST и другие интерфейсы Supabase) боту не нужно.

Что это добавляет (проверьте текущий состав и условия в документации Supabase):

- веб-панель с редактором SQL и просмотром таблиц;
- управление ролями и правами для разных людей и агентов;
- у облачного проекта – резервные копии и обслуживание базы на стороне провайдера (условия зависят от тарифа);
- интерфейсы доступа к данным поверх базы (бот ими не пользуется; о риске их публикации таблиц бота – раздел 9.2).

Во что это обходится по памяти и обслуживанию:

- облачный проект: память на вашем сервере не расходуется, но данные участников чатов хранятся у провайдера в выбранном регионе, а условия оплаты определяет тариф;
- self-hosted: это набор из многих контейнеров, он требует заметно больше оперативной памяти и внимания, чем один голый PostgreSQL. Конкретные требования смотрите в документации Supabase по self-hosting; цифр здесь нет намеренно. Обновления и бэкапы остаются на вас.

Как подключить бота – раздел 9.1 и 9.2: отдельная роль, отдельная схема, строка подключения в `ARCHIVE_DATABASE_DSN`, запуск сервисов бота с `--no-deps`.

### Сравнение

| Размер проекта | Вариант | Что получаете | Во что обходится (память, обслуживание) | Когда менять |
|---|---|---|---|---|
| Малый: один владелец, несколько чатов, маленький VPS | (a) один контейнер PostgreSQL с настройками выше | Всё в одном `docker compose`; нет сторонних сервисов; доступ только через `psql`, HTTP API и роль для чтения | Минимум: один контейнер PostgreSQL с лимитом памяти (по умолчанию 384m); бэкапы и обновления на вас (раздел 7.4) | Когда настройки памяти уже подняты, а запросы всё равно упираются в сервер; нужен общий интерфейс для команды; нужны разные права разным людям; данные не помещаются на диск сервера |
| Средний: больше чатов, один сервер, свои админы | (a) с повышенными `PG_*` или (c) существующий PostgreSQL | Больше памяти базе или общая база со своими бэкапами и мониторингом | Память и диск сервера; обслуживание – ваше или администратора базы | Когда нужна панель управления и роли без собственного администрирования PostgreSQL |
| Большой: много чатов, команда, аналитика, несколько агентов | (b) свой Supabase, облачный или self-hosted | Панель, роли и права, средства провайдера для копий; бот работает с тем же PostgreSQL | Облако: деньги и данные у провайдера. Self-hosted: много контейнеров и заметно больше RAM, чем у одного PostgreSQL (см. документацию Supabase) | Обратно, на маленький вариант – если проект уменьшился; те же команды, что и ниже |

### Как переехать с малого варианта на большой без потери архива

Курсор чтения Telegram (`poll_state.next_offset`), все сообщения, правки, вложения, расшифровки и решения владельца лежат в базе, поэтому перенос базы переносит и положение бота. Файлы вложений остаются в каталоге `media` на сервере бота и в базу не переезжают.

1. Подготовьте целевую базу (раздел 9.1): роль, схема или отдельная база. Убедитесь, что утилиты `pg_dump` и `pg_restore` на хосте не старше версии PostgreSQL сервера.
2. Остановите чтение, чтобы в базу не писали во время переноса. Telegram хранит неполученные обновления ограниченное время (по документации Bot API около суток, проверьте актуальное значение), поэтому не затягивайте:

   ```sh
   docker compose stop poller worker api
   ```

3. Снимите финальную копию:

   ```sh
   docker compose exec -T db pg_dump -U archive -Fc --no-owner --no-privileges archive > move.dump
   ```

4. Восстановите её в целевую базу. Два варианта, оба проверены локально обычными утилитами PostgreSQL 16:

   **Вариант А – таблицы в схеме `public` пустой отдельной базы** (например, отдельная база на внешнем PostgreSQL):

   ```sh
   pg_restore --exit-on-error --no-owner --no-privileges -d "$TARGET_DSN" move.dump
   ```

   **Вариант Б – таблицы в отдельной схеме `oko`** (рекомендуется для Supabase, раздел 9.1). Дамп содержит схему `public`, поэтому сначала переименуйте её во временной базе:

   ```sh
   docker compose exec -T db createdb -U archive scratch
   docker compose exec -T db pg_restore -U archive -d scratch --exit-on-error --no-owner --no-privileges < move.dump
   docker compose exec -T db psql -U archive -d scratch -c 'ALTER SCHEMA public RENAME TO oko' -c 'CREATE SCHEMA public'
   docker compose exec -T db pg_dump -U archive -Fc --no-owner --no-privileges -n oko scratch > move_oko.dump
   pg_restore -l move_oko.dump | grep -v -e ' SCHEMA - oko ' -e 'COMMENT - SCHEMA oko' > move_oko.list
   pg_restore --exit-on-error --no-owner --no-privileges -L move_oko.list -d "$TARGET_DSN" move_oko.dump
   docker compose exec -T db dropdb -U archive scratch
   ```

   Файл `move_oko.list` убирает из восстановления команду создания схемы: схему `oko` вы создали заранее (9.1), и у роли бота нет права создавать схемы.

5. Поменяйте `ARCHIVE_DATABASE_DSN` в `.env` на строку подключения к новой базе. Пока этого не сделано, `migrate` и `status` работают со старой базой и ничего не говорят о новой. Старый контейнер `db` и `move.dump` не удаляйте несколько дней: это путь отката (верните прежнюю строку подключения).
6. Проверьте результат уже на новой базе. Сравните число строк (`SELECT count(*) FROM messages` в обеих базах), выполните `docker compose run --rm --no-deps ops migrate` (должно напечатать `migration_ok` без изменений) и `docker compose run --rm --no-deps ops status`. Счётчики идентификаторов переносятся: на тестовой базе `events_id_seq` после восстановления равен максимальному `events.id`. Затем запустите сервисы без контейнера `db`: `docker compose up -d --no-deps api poller worker`.
7. Проверьте живую работу: отправьте тестовое сообщение в одобренный чат и найдите его в новой базе.

Обратный переезд (с большого на малый) выполняется теми же командами в другую сторону.

Содержание (сначала решение «маленький или большой» выше, затем разделы):

1. Какой вариант базы выбрать
2. Вариант (a): PostgreSQL в Docker рядом с ботом – по шагам
3. Где лежат файлы и сколько диска планировать
4. Схема базы человеческим языком
5. Готовые запросы для владельца и для AI-агента
6. Доступ для AI-агента и аналитики
7. Хранение и удаление данных, резервные копии
8. Безопасность
9. Варианты (b) и (c): Supabase и внешний PostgreSQL

## 1. Какой вариант базы выбрать

| Вариант | Когда выбирать | Что сделать |
|---|---|---|
| (a) PostgreSQL в Docker рядом с ботом (сервис `db` из `compose.yaml`) | По умолчанию. Один сервер, один `docker compose`, база не видна из интернета. | Заполнить `POSTGRES_PASSWORD` и `ARCHIVE_DATABASE_DSN` в `.env`, выполнить `migrate`. |
| (b) Ваш проект Supabase (облачный или self-hosted) как PostgreSQL | Нужна управляемая база с бэкапами провайдера или вы уже работаете в Supabase. | Создать отдельную роль и схему, вписать строку подключения в `ARCHIVE_DATABASE_DSN`, запускать сервисы бота без контейнера `db`. См. раздел 9. |
| (c) Уже существующий PostgreSQL на другом сервере | У вас есть общий сервер баз данных со своими бэкапами и мониторингом. | Создать отдельную базу и пользователя, открыть доступ только с сервера бота, включить TLS. См. раздел 9. |

Общее для всех вариантов:

- Бот подключается к базе обычным драйвером PostgreSQL (psycopg 3). Работа с базой – только через SQL, никакого ORM и никаких расширений PostgreSQL не требуется (в миграциях нет `CREATE EXTENSION`).
- Проверялось на PostgreSQL 16. Другие версии мастер-ветка не тестировала.
- Поллер (процесс, читающий Telegram) держит одно долгоживущее соединение и берёт на нём сеансовую блокировку `pg_try_advisory_lock`, чтобы читать Telegram мог только один процесс. Поэтому поллеру нужно прямое соединение с PostgreSQL или пулер в режиме «сессия». Пулер в режиме «транзакция» не подходит – это следует из кода.
- Бот при подключении сам задаёт `statement_timeout=30s` и `lock_timeout=5s` через параметр запуска `options`. Не все пулеры принимают этот параметр – проверьте в документации вашего пулера.

## 2. Вариант (a): PostgreSQL в Docker рядом с ботом – по шагам

Предполагается, что вы уже склонировали репозиторий и создали `.env` из `.env.example` (см. README, «Быстрый старт»).

**Шаг 1. Каталоги данных.** Все данные лежат в одном каталоге `OKO_DATA_DIR` (по умолчанию в примере `/var/lib/oko`), вне каталога репозитория:

```sh
sudo mkdir -p /var/lib/oko/db /var/lib/oko/media /var/lib/oko/exports
sudo chown 999:999 /var/lib/oko/db                      # пользователь postgres в контейнере
sudo chown 10001:10001 /var/lib/oko/media /var/lib/oko/exports   # пользователь приложения
sudo chmod 700 /var/lib/oko/db /var/lib/oko/media /var/lib/oko/exports
```

**Шаг 2. База и пользователь.** Сервис `db` стартует с профилем памяти для маленького сервера (раздел «Выбор: маленький проект или большой»). Образ `postgres` при первом запуске сам создаёт базу `archive` и пользователя `archive` с паролем из `POSTGRES_PASSWORD` (так заданы переменные `POSTGRES_DB` и `POSTGRES_USER` в `compose.yaml`). Пароль в `ARCHIVE_DATABASE_DSN` должен совпадать с `POSTGRES_PASSWORD`:

```sh
openssl rand -hex 24      # получите пароль и впишите его в оба места в .env
```

В `.env`:

```
POSTGRES_PASSWORD=<пароль>
ARCHIVE_DATABASE_DSN=postgresql://archive:<тот же пароль>@db:5432/archive
```

Важно: в официальном образе `POSTGRES_USER` становится суперпользователем внутри контейнера. Порт базы наружу не публикуется (в `compose.yaml` у сервиса `db` нет `ports`), но для разделения прав создайте отдельные роли для чтения (раздел 6) и не выдавайте их владельцу.

**Шаг 3. Запуск базы и миграции.**

```sh
docker compose up -d db
docker compose ps db                      # дождитесь состояния healthy
docker compose run --rm ops migrate       # печатает: migration_ok
```

`migrate` – единственная команда, которая создаёт и обновляет таблицы. Она накладывает файлы `migrations/001_*.sql` … `005_*.sql` по порядку и записывает применённые номера в таблицу `schema_version`. Повторный запуск безопасен: уже применённые номера пропускаются. Во время миграции берётся блокировка, поэтому две одновременные миграции не столкнутся.

Без Docker та же команда выглядит так (нужен Python 3.12+ и `uv sync`):

```sh
ARCHIVE_DATABASE_DSN='postgresql://…' ARCHIVE_BOT_TOKEN='…' \
  .venv/bin/python -m archive --config settings.toml migrate
```

**Шаг 4. Проверка, что таблицы на месте.**

```sh
docker compose exec db psql -U archive -d archive -c '\dt'
docker compose exec db psql -U archive -d archive -c 'SELECT version FROM schema_version ORDER BY 1'
```

Должно быть 14 таблиц (список – в разделе 4) и версии 1–5. Эту же проверку на тестовой базе выполнил автор документа: `migrate` создаёт ровно эти 14 таблиц, повторный `migrate` печатает `migration_ok` и ничего не меняет.

**Шаг 5. Запуск бота.**

```sh
docker compose up -d
docker compose run --rm ops status        # JSON: поллер, очередь задач, файлы, число чатов по статусам
```

Чтобы узнать числовой ID чата для запросов и экспорта:

```sh
docker compose exec db psql -U archive -d archive -c 'SELECT chat_id, title, status FROM chat_approvals ORDER BY requested_at'
```

## 3. Где лежат файлы и сколько диска планировать

**Что где.**

- В PostgreSQL: тексты, правки, метаданные вложений, транскрипции и полный сырой JSON каждого обновления Telegram.
- На диске, в `media_root` (в контейнере `/var/lib/archive-media`, на хосте `$OKO_DATA_DIR/media`): сами файлы вложений. Путь файла: `<slug чата>/<ID чата>/<дата получения UTC>/<ID вложения>/<идентификатор задачи>/file`; относительный путь записан в `attachments.relative_path`. Имя файла от отправителя в путь не попадает. Файлы пишет только контейнер `worker`; контейнер `api` монтирует каталог только для чтения.
- Объектное хранилище (S3, Supabase Storage) в коде **не реализовано**: файлы можно хранить только на локальном диске или на смонтированном в него томе.
- Файл больше 20 МиБ (20 971 520 байт) не скачивается: лимит облачного Bot API. В базе остаётся запись о вложении со статусом `oversized_not_downloaded`.
- Результаты `export` пишутся в `export_root` (хост: `$OKO_DATA_DIR/exports`).

**Сколько диска.** Фиксированных цифр нет, они зависят от вашего чата. Замерьте:

```sql
-- размер таблиц базы
SELECT relname AS table, pg_size_pretty(pg_total_relation_size(c.oid)) AS size
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = current_schema() AND c.relkind = 'r'   -- схема из search_path: public или oko
ORDER BY pg_total_relation_size(c.oid) DESC;

-- файлы на диске по типам
SELECT kind, count(*) AS files, pg_size_pretty(coalesce(sum(size), 0)) AS bytes
FROM attachments WHERE status = 'ready' GROUP BY kind ORDER BY kind;

-- число обновлений, из которых сложился размер
SELECT count(*) AS events FROM events;
```

Формула для прогноза:

- база на период T ≈ (размер всех таблиц ÷ число строк в `events`) × ожидаемое число обновлений за T. Замеряйте после недели работы, а не по первому дню;
- файлы на период T ≈ ожидаемое число файлов × средний размер файла из запроса выше; один файл не больше 20 МиБ;
- резервные копии ≈ (размер дампа + размер `media`) × число хранимых копий;
- к сумме добавьте запас на рост между проверками. Размер запаса выберите сами; код его не навязывает.

Строка `events.raw` хранит полный JSON каждого обновления, поэтому таблица `events` обычно самая крупная из текстовых.

## 4. Схема базы человеческим языком

Связи таблиц. «->» значит «ссылается на» (внешний ключ), «..>» – логическая связь без внешнего ключа.

```
    chat_approvals ..> messages      по chat_id
    messages  -> events              по event_id (событие текущей версии)
    revisions -> events              по event_id (событие каждой версии)
    messages ..> revisions           по (chat_id, message_id)
    attachments -> events            по event_id
    messages ..> attachments         по (chat_id, message_id)
    transcripts -> attachments       по attachment_id (одна расшифровка на вложение)
    jobs -> attachments              по attachment_id (задачи скачивания и расшифровки)
```

Таблицы (всего 14; созданы в `migrations/001`…`005`):

| Таблица | Что хранит | Ключевые столбцы |
|---|---|---|
| `events` | Каждое принятое обновление Telegram целиком. Основа для проверки и повторной обработки. | `id`; `fingerprint` (SHA-256 канонического JSON, уникален: повторная доставка не создаёт дубль); `update_id`; `chat_id`; `raw` (канонический JSON в формате bytea, только ASCII); `error_code` (не пусто, если обновление не удалось разобрать и оно помещено в карантин); `received_at`. |
| `messages` | Текущая версия каждого сообщения. | PK (`chat_id`, `message_id`); `sent_at` (время отправки); `author_id` (числовой ID автора, пусто у постов канала); `text` (текст или подпись к медиа, пустая строка для сообщений без текста, в том числе служебных); `reply_to_id`; `media_group_id` (альбом); `revision_time` (время последней правки в секундах Unix); `event_id` (обновление, из которого взята текущая версия). |
| `revisions` | Все версии сообщения: исходная и каждая правка. | PK `event_id`; `chat_id`; `message_id`; `revision_time`; `text`. Связь с `messages` по паре (`chat_id`, `message_id`). |
| `attachments` | Метаданные вложений: фото, документ, голосовое, аудио, видео, кружок, анимация, стикер, превью. | `id`; `event_id`; `chat_id`; `message_id`; `slot` (например `voice`, `photo/0`); `file_id`, `file_unique_id` (идентификаторы Telegram); `declared_size`, `mime`, `kind`; `status` (`pending`, `ready`, `failed`, `blocked`, `oversized_not_downloaded`); `error_code`; `relative_path`, `size`, `sha256` (заполнены, когда файл скачан). |
| `transcripts` | Расшифровка голосовых, аудио и видео. | PK `attachment_id`; `status` (`pending`, `blocked`, `submitting`, `uploaded`, `polling`, `ready`, `failed`, `reconciliation_required`, `unavailable`); `provider` (`groq` или `assemblyai`); `text`; `error_code`. Поля `submission_id`, `uploaded_url` – служебные данные внешнего сервиса. |
| `chat_approvals` | Решение владельца по каждому чату. | PK `chat_id`; `status` (`pending`, `active`, `declined`, `stopped`, `removed`, `expired`); `title` (название чата, до 128 символов); `kind`; `requested_at`, `approved_at`, `approved_by`, `decided_by`, `updated_at`; `dropped` (сколько обновлений отброшено без сохранения, пока чат не включён); `migrated_to` (новый ID, если группа стала супергруппой). |
| `read_audit` | Журнал чтений через HTTP API (раздел «Что пишется в журналы» в README). | `at`; `agent` (имя ключа, пусто для неизвестного токена); `route` (шаблон пути); `chat_id`; `status` (HTTP-код); `result_count`. |
| `jobs` | Очередь фоновой работы: скачивание файлов, расшифровка, служебные сообщения владельцу. | `kind` (`download`, `transcribe`, `transcript_poll`, `leave`, `unknown_digest`, `founder_alert`, `approval_prompt`, `owner_callback`, `chats_list`); `status` (`pending`, `running`, `done`, `failed`); `attempts`, `next_run`, `error_code`; `payload` (JSON с числовыми ID и кодами, без текстов сообщений). |
| `poll_state` | Одна строка: курсор чтения Telegram. | `next_offset`, `epoch`, `last_poll`, `last_commit`, `failures`. Столбец `sidecar_session` исторически называется так, хранит метку транспорта. |
| `mapping_failures` | Счётчик неудачных разборов одного и того же обновления (после трёх попыток обновление уходит в карантин). | `fingerprint`, `attempts`. |
| `unknown_chats` | Числовые ID чатов, где бот замечен, но владелец их не одобрял. Срок хранения – 24 часа. | `chat_id`, `kind`, `expires_at`. |
| `enrollment_hints` | Пары «старый ID – новый ID» при переходе группы в супергруппу. | `old_chat_id`, `new_chat_id`. |
| `notification_state` | Одна строка: когда последний раз отправлялась сводка о неизвестных чатах. | `last_attempt`. |
| `schema_version` | Номера применённых миграций. | `version`. |

Индексы, которые важны для выгрузки и поиска (созданы миграциями):

- `messages`: первичный ключ (`chat_id`, `message_id`) – быстрый доступ к сообщению и выборка чата по порядку; индекс `messages_date` (`chat_id`, `sent_at`, `message_id`) – выборка чата за период;
- `events`: индекс `events_chat` (`chat_id`, `id`) и уникальный `fingerprint`;
- `attachments`: уникальный (`event_id`, `slot`);
- `read_audit`: `read_audit_at` (`at`) и `read_audit_agent` (`agent`, `at`);
- `jobs`: `jobs_pending` (`status`, `next_run`, `kind`); `chat_approvals`: `chat_approvals_status` (`status`, `requested_at`).

Индексов по `author_id`, по `revisions(chat_id, message_id)` и по `attachments(chat_id, message_id)` миграции не создают, как и индекса для поиска по тексту (HTTP API ищет по подстроке полным просмотром). Если такие запросы медленные на вашем объёме, индексы можно добавить отдельной командой. Миграции репозитория их не затрагивают, но проверьте, что новые индексы не конфликтуют с будущими миграциями.

## 5. Готовые запросы для владельца и для AI-агента

Подключение: `docker compose exec db psql -U archive -d archive` (владелец) или `psql` под ролью для чтения (раздел 6). ID чата в примерах `-1001234567890`, время – в UTC. Перед запросами по времени удобно выполнить `SET TIME ZONE 'UTC';`. Запросы не указывают схему и рассчитаны на сессию, где `search_path` ведёт к схеме бота: во встроенной базе это `public`, под ролью бота из 9.1 – `oko`. Под другой ролью сначала выполните `SET search_path = oko;`.

**Сообщения одного чата за период**

```sql
SELECT message_id, sent_at, author_id, left(text, 80) AS text
FROM messages
WHERE chat_id = -1001234567890
  AND sent_at >= '2025-10-09 00:00:00+00' AND sent_at < '2025-10-10 00:00:00+00'
ORDER BY sent_at, message_id;
```

**Кто писал больше всех**

```sql
SELECT author_id, count(*) AS messages, min(sent_at) AS first_at, max(sent_at) AS last_at
FROM messages
WHERE chat_id = -1001234567890 AND author_id IS NOT NULL
GROUP BY author_id ORDER BY messages DESC LIMIT 10;
```

**Имена авторов.** Имя и @username отдельными столбцами не хранятся; они лежат в сыром JSON в `events.raw`:

```sql
SELECT DISTINCT ON (m.author_id)
       m.author_id,
       trim(concat_ws(' ', b->'from'->>'first_name', b->'from'->>'last_name')) AS name,
       b->'from'->>'username' AS username
FROM messages m
JOIN events e ON e.id = m.event_id
CROSS JOIN LATERAL (
  SELECT value AS b
  FROM jsonb_each(convert_from(e.raw, 'UTF8')::jsonb)
  WHERE key IN ('message', 'edited_message', 'channel_post', 'edited_channel_post')
) x
WHERE m.chat_id = -1001234567890 AND m.author_id IS NOT NULL
ORDER BY m.author_id, m.sent_at DESC;
```

Имя берётся из последней версии сообщения. Если в самом JSON встретится символ `\u0000`, приведение к `jsonb` завершится ошибкой; такие обновления редки, для них используйте приведение к `json`.

**Сообщения без ответа.** Здесь «без ответа» значит: после сообщения клиента в чате не было ни одного сообщения вашей команды. В `staff` перечислите ID своих сотрудников (владельца тоже):

```sql
WITH staff(id) AS (VALUES (111111111::bigint), (444444444::bigint))
SELECT m.message_id, m.sent_at, m.author_id, left(m.text, 80) AS text
FROM messages m
WHERE m.chat_id = -1001234567890
  AND m.author_id IS NOT NULL
  AND m.author_id NOT IN (SELECT id FROM staff)
  AND NOT EXISTS (
    SELECT 1 FROM messages r
    WHERE r.chat_id = m.chat_id AND r.author_id IN (SELECT id FROM staff)
      AND (r.sent_at, r.message_id) > (m.sent_at, m.message_id))
ORDER BY m.sent_at;
```

Запрос не учитывает, на какое именно сообщение ответили, и не различает ответ по существу и реплику «ок».

**Поиск по тексту и по расшифровкам**

```sql
SELECT m.chat_id, m.message_id, m.sent_at, left(m.text, 80) AS text
FROM messages m
WHERE strpos(lower(m.text), lower('счёт')) > 0
   OR EXISTS (SELECT 1 FROM attachments a JOIN transcripts t ON t.attachment_id = a.id
              WHERE a.chat_id = m.chat_id AND a.message_id = m.message_id
                AND strpos(lower(t.text), lower('счёт')) > 0)
ORDER BY m.sent_at;
```

Так же ищет и HTTP API (`/v1/search`): буквальная подстрока без регулярных выражений, поэтому символы `%` и `_` не требуют экранирования.

**История правок сообщения**

```sql
SELECT to_timestamp(revision_time) AS at, text
FROM revisions WHERE chat_id = -1001234567890 AND message_id = 4
ORDER BY revision_time, event_id;
```

**Вложения и расшифровки**

```sql
SELECT a.id, a.message_id, a.kind, a.status, a.size, t.status AS transcript_status, left(t.text, 80) AS transcript
FROM attachments a LEFT JOIN transcripts t ON t.attachment_id = a.id
WHERE a.chat_id = -1001234567890 ORDER BY a.id;
```

**Ответы и на что отвечали**

```sql
SELECT m.message_id, left(m.text, 40) AS text, m.reply_to_id, left(p.text, 40) AS parent
FROM messages m JOIN messages p ON p.chat_id = m.chat_id AND p.message_id = m.reply_to_id
WHERE m.chat_id = -1001234567890;
```

**Сообщений по дням**

```sql
SELECT date_trunc('day', sent_at) AS day, count(*) FROM messages
WHERE chat_id = -1001234567890 GROUP BY 1 ORDER BY 1;
```

**Выгрузка в CSV через `psql \copy`.** Вариант с файлом (файл создаётся на машине, где запущен `psql`):

```sql
\copy (SELECT chat_id, message_id, sent_at, author_id, reply_to_id, text FROM messages WHERE chat_id = -1001234567890 AND sent_at >= '2025-10-01+00' AND sent_at < '2025-11-01+00' ORDER BY sent_at, message_id) TO 'messages.csv' WITH (FORMAT csv, HEADER)
```

Вариант для контейнера: вывод идёт в стандартный поток, и файл создаёт ваша оболочка на хосте:

```sh
docker compose exec -T db psql -U archive -d archive -c "\copy (SELECT chat_id, message_id, sent_at, author_id, reply_to_id, text FROM messages WHERE chat_id = -1001234567890 ORDER BY sent_at, message_id) TO STDOUT WITH (FORMAT csv, HEADER)" > messages.csv
```

Оба варианта проверены через локальный `psql` (с `TO STDOUT` – тоже локально, без Docker). Выгрузить данные можно и через HTTP API (README, раздел «Выгрузка данных»).

Результат первых запросов на тестовых данных выглядит так (авторы и тексты синтетические):

```
 author_id | messages |        first_at        |        last_at
-----------+----------+------------------------+------------------------
       222 |        3 | 2025-10-09 09:03:20+00 | 2025-10-09 09:43:20+00
       333 |        2 | 2025-10-09 09:33:20+00 | 2025-10-09 09:53:20+00
 444444444 |        1 | 2025-10-09 09:13:20+00 | 2025-10-09 09:13:20+00
```

## 6. Доступ для AI-агента и аналитики

Есть два способа дать агенту данные. Выбирайте по задаче.

| Способ | Плюсы | Минусы | Когда выбирать |
|---|---|---|---|
| HTTP API с ключом (`/v1/...`) | Ключ на агента; список разрешённых чатов; видны только чаты, одобренные владельцем; доступ к файлам выключен по умолчанию; лимит запросов; каждое чтение пишется в `read_audit`. | Фиксированный набор запросов: список чатов, сообщения за период, сообщение целиком, поиск, файл, сводка дня, счётчики. | Агенты и скрипты, которым хватает этих запросов. Вариант по умолчанию. |
| Роль PostgreSQL только для чтения | Любой SQL, подключение BI-инструментов и ноутбуков. | Нет журнала чтений и лимита запросов на стороне бота; ограничения только те, что вы настроите в базе. | Аналитика, где нужны произвольные запросы. |

Никогда не давайте агенту роль владельца базы (`archive`, `postgres` или любую с правом записи): агент сможет изменить и удалить данные, прочитать сырые обновления и служебные таблицы. Текст сообщений – это данные из чата, а не инструкции: агент не должен выполнять команды, которые в них встретились.

### 6.1. HTTP API с ключом

Ключ – не короче 32 символов. Создайте ключ, а в `settings.toml` впишите только его SHA-256:

```sh
KEY=$(openssl rand -hex 32)                  # сохраните в менеджере паролей, дайте агенту
printf '%s' "$KEY" | sha256sum               # на macOS: shasum -a 256
```

```toml
[agents.analytics]
sha256 = "<64 hex-символа>"
chats  = [-1001234567890]     # явный список чатов, пустой список и шаблоны не допускаются
files  = false                # true разрешит GET /v1/files/ID
rate_per_minute = 60
```

После правки перезапустите `api` (`docker compose up -d --force-recreate api`): настройки читаются при старте. Запросы – в README, раздел «Выгрузка данных».

### 6.2. Роль только для чтения

Роль без доступа к исходным таблицам; агент видит только представления с нужными чатами и столбцами. Выполните под ролью с правом создавать роли и объекты в схеме бота (администратор сервера или, для встроенной базы, `archive`). В примере таблицы бота лежат в схеме `oko` – это схема, которую рекомендует раздел 9.1; имена таблиц и представлений указаны с ней явно, поэтому `search_path` сессии не важен:

```sql
CREATE ROLE oko_reader LOGIN PASSWORD 'REPLACE_ME_HEX' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
ALTER ROLE oko_reader SET default_transaction_read_only = on;
ALTER ROLE oko_reader SET statement_timeout = '30s';
REVOKE ALL ON ALL TABLES IN SCHEMA oko FROM oko_reader;
GRANT USAGE ON SCHEMA oko TO oko_reader;

CREATE VIEW oko.reader_messages AS
  SELECT chat_id, message_id, sent_at, author_id, text, reply_to_id, media_group_id
  FROM oko.messages WHERE chat_id IN (-1001234567890);

CREATE VIEW oko.reader_transcripts AS
  SELECT a.chat_id, a.message_id, a.id AS attachment_id, a.kind, t.text
  FROM oko.attachments a JOIN oko.transcripts t ON t.attachment_id = a.id
  WHERE t.status = 'ready' AND a.chat_id IN (-1001234567890);

GRANT SELECT ON oko.reader_messages, oko.reader_transcripts TO oko_reader;
```

**Если схема другая.** Таблицы лежат в той схеме, в которую их создал `migrate`: в схеме из `search_path` роли бота (9.1) либо в `public`, если `search_path` не задавали, – так во встроенной базе (вариант (a)) и в отдельной базе без схемы. Тогда замените `oko` на имя своей схемы во всех строках блока (`REVOKE`, `GRANT USAGE`, оба `CREATE VIEW`, `GRANT SELECT`). Проверить схему можно запросом `SELECT schemaname FROM pg_tables WHERE tablename = 'messages'`. Роли `oko_reader` не нужен `search_path`: читателю достаточно писать `SELECT … FROM oko.reader_messages`.

Проверено на тестовой базе (схема `oko`, `migrate` выполнен под ролью с `search_path = oko`) под ролью `oko_reader`: запросы к представлениям работают; `SELECT` из `oko.messages`, `oko.events` и `oko.chat_approvals` отвечает `permission denied`; `DELETE` из представления отвечает `permission denied`; `CREATE TABLE` отвечает `cannot execute CREATE TABLE in a read-only transaction`.

Роль намеренно не получает `events` (там полные сырые обновления с именами и @username), `jobs`, `read_audit` и `chat_approvals`. Если агенту нужны имена, добавьте представление с нужными полями сами, взяв запрос «Имена авторов» из раздела 5. При добавлении нового чата дополните список `IN (...)` в представлениях (`CREATE OR REPLACE VIEW`).

Не раскрывайте базу в интернет ради агента: подключайтесь к ней через SSH-туннель или частную сеть (раздел 8).

## 7. Хранение и удаление данных, резервные копии

### 7.1. Сколько хранить

Код не удаляет сообщения сам: ни по сроку, ни по объёму. Автоматически очищаются только служебные данные: `unknown_chats` и просроченные задачи в `jobs` (по сроку действия), а журнал `read_audit` вы чистите командой:

```sh
docker compose run --rm ops audit prune --days 180      # печатает: pruned N
```

Срок хранения сообщений решаете вы. Если закон или ваши правила требуют ограничить срок, добавьте регулярный запуск удаления по образцу из 7.2 (за пределами репозитория, например через cron).

### 7.2. Удалить данные одного чата

1. В боте остановите сбор: `/chats` → карточка чата → «Стоп» → подтверждение. Иначе сообщения продолжат поступать. Если чат больше не нужен, уберите из него бота и удалите его из `chats` в `[agents.*]`.
2. Запишите, какие каталоги файлов относятся к чату (до удаления строк):

```sql
SELECT DISTINCT split_part(relative_path, '/', 1) || '/' || split_part(relative_path, '/', 2) AS media_dir
FROM attachments WHERE chat_id = -1001234567890 AND relative_path IS NOT NULL;
```

3. Удалите строки одной транзакцией. Порядок важен из-за внешних ключей:

```sql
BEGIN;
DELETE FROM transcripts WHERE chat_id = -1001234567890;
DELETE FROM jobs        WHERE chat_id = -1001234567890;
DELETE FROM attachments WHERE chat_id = -1001234567890;
DELETE FROM revisions   WHERE chat_id = -1001234567890;
DELETE FROM messages    WHERE chat_id = -1001234567890;
DELETE FROM events      WHERE chat_id = -1001234567890;
COMMIT;
```

Если группа переходила в супергруппу, у неё два ID (`SELECT chat_id FROM chat_approvals WHERE migrated_to = -1001234567890`): повторите для старого ID.

4. Удалите файлы на диске, например `sudo rm -rf /var/lib/oko/media/<media_dir из шага 2>`. Проверьте путь глазами до выполнения команды.
5. Строку в `chat_approvals` оставьте в статусе `stopped`. Если её удалить, а бот останется в чате, бот сочтёт чат неизвестным и выйдет из него.

Проверено на копии тестовой базы: после выполнения шага 3 в `messages`, `revisions`, `events`, `attachments`, `jobs` по чату нет строк, в `chat_approvals` строка осталась.

Что остаётся после удаления: резервные копии (удалите или дождитесь ротации, раздел 7.3), записи `read_audit` (в них нет текстов), место на диске (его возвращает автоочистка PostgreSQL для повторного использования; чтобы вернуть место системе, нужен `VACUUM FULL` на остановленном боте).

### 7.3. Удалить данные одного человека по его просьбе

Скажите человеку, что удалить можно сообщения, которые он написал, но не то, как их цитируют другие. Telegram хранит свои копии независимо от бота.

Сначала найдите числовой ID человека (раздел 5, «Имена авторов») и посмотрите, где он ещё упоминается:

```sql
-- обновления, где его ID встречается где угодно: ответы других людей на его сообщения, пересылки
SELECT e.chat_id, count(*) AS events
FROM events e
WHERE convert_from(e.raw, 'UTF8') ~ '"id":222[,}]'
GROUP BY e.chat_id;
```

Удаление его собственных сообщений (подставьте ID вместо `222`; это можно ограничить одним чатом условием `AND chat_id = …`). Сначала запишите каталоги файлов запросом `SELECT DISTINCT split_part(a.relative_path, '/', 1) || '/' || split_part(a.relative_path, '/', 2) FROM attachments a JOIN messages m USING (chat_id, message_id) WHERE m.author_id = 222 AND a.relative_path IS NOT NULL`; обратите внимание, что в каталоге чата лежат и файлы других людей, поэтому удаляйте по `relative_path` отдельных вложений, а не весь каталог.

```sql
BEGIN;
CREATE TEMP TABLE doomed ON COMMIT DROP AS
  SELECT chat_id, message_id FROM messages WHERE author_id = 222;
CREATE TEMP TABLE doomed_files ON COMMIT DROP AS
  SELECT a.id FROM attachments a JOIN doomed d USING (chat_id, message_id);
CREATE TEMP TABLE doomed_events ON COMMIT DROP AS
  SELECT r.event_id FROM revisions r JOIN doomed d USING (chat_id, message_id)
  UNION
  SELECT m.event_id FROM messages m JOIN doomed d USING (chat_id, message_id)
  UNION
  SELECT a.event_id FROM attachments a JOIN doomed_files f ON f.id = a.id;
DELETE FROM transcripts WHERE attachment_id IN (SELECT id FROM doomed_files);
DELETE FROM jobs        WHERE attachment_id IN (SELECT id FROM doomed_files);
DELETE FROM attachments WHERE id IN (SELECT id FROM doomed_files);
DELETE FROM revisions r USING doomed d WHERE r.chat_id = d.chat_id AND r.message_id = d.message_id;
DELETE FROM messages  m USING doomed d WHERE m.chat_id = d.chat_id AND m.message_id = d.message_id;
DELETE FROM events WHERE id IN (SELECT event_id FROM doomed_events);
COMMIT;
```

Проверено на копии тестовой базы: у автора с тремя сообщениями, одним вложением и одной расшифровкой после выполнения не осталось ни сообщений, ни правок, ни вложений, ни расшифровок.

Сообщения других людей, которые отвечают ему или пересылают его текст, хранят копию в своём `events.raw` (в тесте осталось одно такое обновление). Обезличить `raw` на месте нельзя: HTTP API ожидает там канонический JSON. Если нужно убрать и эти копии, удалите такие сообщения целиком, заменив выборку `doomed` на:

```sql
CREATE TEMP TABLE doomed ON COMMIT DROP AS
  SELECT chat_id, message_id FROM messages WHERE author_id = 222
  UNION
  SELECT m.chat_id, m.message_id FROM messages m JOIN events e ON e.id = m.event_id
  WHERE convert_from(e.raw, 'UTF8') ~ '"id":222[,}]';
```

Упоминания имени человека внутри чужого текста SQL не найдёт; смотрите их поиском по тексту (раздел 5).

### 7.4. Резервные копии

База (формат custom, годится для `pg_restore`):

```sh
docker compose exec -T db pg_dump -U archive -Fc archive > oko-$(date -u +%Y%m%dT%H%M%SZ).dump
```

Файлы вложений (создаются один раз и не меняются, поэтому копия остаётся согласованной с базой; новые файлы попадут в следующую копию):

```sh
sudo tar -C /var/lib/oko -cf oko-media-$(date -u +%Y%m%dT%H%M%SZ).tar media
```

Копировать каталог `db/` вместо дампа нельзя, пока база работает: копия получится несогласованной.

Проверка, что копия восстанавливается (делайте не реже раза в квартал и после смены версии PostgreSQL):

```sh
pg_restore --list oko-….dump > /dev/null                       # оглавление читается
docker compose exec -T db createdb -U archive oko_restore_check
docker compose exec -T db pg_restore -U archive -d oko_restore_check --exit-on-error --no-owner < oko-….dump
docker compose exec -T db psql -U archive -d oko_restore_check -c 'SELECT count(*) FROM messages'
docker compose exec -T db dropdb -U archive oko_restore_check
```

Сравните число сообщений с рабочей базой. Если `pg_restore` не установлен на хосте, замените первую команду на `docker compose exec -T db pg_restore --list < oko-….dump`.

На тестовой базе проверено то же самое локальными `pg_dump -Fc`, `pg_restore --list`, `pg_restore --file=/dev/null` и восстановлением в пустую базу: число строк в `messages`, `revisions`, `events`, `attachments`, `transcripts`, `chat_approvals` совпало, `migrate` на восстановленной базе напечатал `migration_ok`.

Восстановление в рабочую систему: остановите `poller`, `worker`, `api`; пересоздайте базу (`dropdb`, `createdb`); выполните `pg_restore --no-owner -d archive < копия`; распакуйте `media` в `$OKO_DATA_DIR` и верните владельца `sudo chown -R 10001:10001 /var/lib/oko/media`; запустите сервисы. Сообщения, написанные между копией и сбоем, Telegram отдаёт боту только ограниченное время (по документации Bot API около суток; проверьте актуальное значение): поллер продолжит с последнего сохранённого смещения и заберёт то, что Telegram ещё не удалил, остальное будет потеряно.

Шифрование копий, если они уходят с сервера: `gpg --symmetric --cipher-algo AES256 oko-….dump` (или `age -p`). Храните пароль от копии отдельно от самой копии. Права на файлы копий – `chmod 600`.

## 8. Безопасность

- База не должна быть доступна из интернета. В `compose.yaml` у сервиса `db` нет опубликованных портов; HTTP API публикуется только на `127.0.0.1:8610`. Если базу нужно открыть другому серверу, используйте частную сеть или SSH-туннель, а в фаерволе разрешите только адрес этого сервера.
- Пароль базы – случайная строка (`openssl rand -hex 24`), хранится только в `.env` (права `chmod 600`, файл в `.gitignore`). Не пишите пароль в командной строке и не публикуйте `docker compose config` с раскрытыми значениями.
- Токен бота и ключи расшифровки – тоже только в `.env`. Файл `settings.toml` секретов не содержит и отклоняется, если в нём есть `ARCHIVE_DATABASE_DSN`, `ARCHIVE_BOT_TOKEN`, `GROQ_API_KEY` или `ASSEMBLYAI_API_KEY`.
- Агенту – ключ API или роль для чтения (раздел 6), не роль владельца.
- Для внешней базы используйте TLS: добавьте в строку подключения `sslmode=verify-full` (или `sslmode=require`, если проверка сертификата невозможна) – это стандартные параметры libpq.
- Резервные копии: права `600`, шифрование при хранении вне сервера, отдельная проверка восстановления (7.4).
- В базе лежат персональные данные участников чатов. Относитесь к ней как к персональным данным; см. раздел «Правила и ответственность» в README.

## 9. Варианты (b) и (c): Supabase и внешний PostgreSQL

### 9.1. Общие шаги для внешней базы

1. Создайте отдельную роль без прав суперпользователя и схему для бота. Схема – не обязательна, но так таблицы бота не смешиваются с чужими. Проверено на PostgreSQL 16: если задать роли `search_path`, `migrate` создаёт все 14 таблиц в этой схеме, а в `public` ничего не появляется:

```sql
CREATE ROLE oko_app LOGIN PASSWORD 'REPLACE_ME_HEX' NOSUPERUSER;
CREATE SCHEMA oko AUTHORIZATION oko_app;
ALTER ROLE oko_app SET search_path = oko;
REVOKE ALL ON SCHEMA public FROM oko_app;
```

   (В отдельной базе, созданной под бота, схема не нужна: `CREATE DATABASE oko OWNER oko_app;`.) `search_path` нужно задавать именно на роли: бот при подключении сам передаёт параметр `options`, и значение из строки подключения затирается.

2. Впишите строку подключения в `.env`:

```
ARCHIVE_DATABASE_DSN=postgresql://oko_app:<пароль>@<хост>:5432/<база>?sslmode=require
```

   Спецсимволы в пароле кодируйте по правилам URL или используйте hex-пароль.

3. Запускайте сервисы бота без контейнера `db`. Флаг `--no-deps` не запускает зависимость `db`:

```sh
docker compose run --rm --no-deps ops migrate
docker compose up -d --no-deps api poller worker
```

   Переменная `POSTGRES_PASSWORD` в этом случае не нужна; `OKO_DATA_DIR` нужна по-прежнему (там лежат `media` и `exports`). Каталоги `media` и `exports` и права на `settings.toml` подготавливаются так же, как для встроенной базы (README, «Быстрый старт», шаг 4), только без каталога `db`: приложение не читает `settings.toml` с правами шире `600`. Остальные команды `docker compose up` и `docker compose run` (`status`, `export`, `audit prune`, `transcripts requeue`, пересоздание сервисов) тоже запускайте с `--no-deps`.

4. Файлы вложений остаются на диске сервера бота (раздел 3), вне зависимости от того, где лежит база. Резервное копирование внешней базы – `pg_dump` с хоста бота (`pg_dump "$ARCHIVE_DATABASE_DSN" -Fc -f oko.dump`) или средства провайдера; проверьте восстановление так же, как в 7.4.

### 9.2. Supabase

Что следует из кода и миграций:

- Бот подключается как обычный клиент PostgreSQL. Ему нужны строка подключения, роль с правом создавать таблицы в своей схеме и сеансовое (прямое либо «session») соединение: поллер держит сеансовую блокировку (раздел 1). Пулер Supabase в режиме «transaction» для поллера и миграций не подходит. Какие порты и хосты соответствуют прямому соединению и режимам пулера, какие из них доступны по IPv4, и принимает ли пулер параметр `options` – проверьте в документации Supabase.
- Строку подключения берите в панели проекта (раздел подключения к базе); формат и актуальные хосты – проверьте в документации Supabase. Пароль в строке должен быть закодирован как часть URL.
- SSL: добавьте `?sslmode=require` в строку. Нужен ли `verify-full` и где взять корневой сертификат – проверьте в документации Supabase.
- Миграции создают обычные таблицы без политик построчного доступа (RLS). Если создавать их в схеме `public` проекта Supabase, они могут оказаться доступны через автоматический REST/GraphQL-интерфейс проекта. Какие схемы он публикует и как это отключить, проверьте в документации Supabase. Безопаснее всего создать отдельную схему (шаг 1 выше) и не включать её в публикуемые, либо включить RLS без политик на всех 14 таблицах (`ALTER TABLE … ENABLE ROW LEVEL SECURITY`), либо отключить Data API у проекта. Эти настройки Supabase в этом репозитории не проверялись.
- Данные участников чатов окажутся у провайдера в выбранном регионе проекта. Учтите это в разделе «Правила и ответственность» (README).
- Для self-hosted Supabase действует то же самое: бот видит его как PostgreSQL. Подключайтесь к порту базы напрямую из частной сети, а не через публичный адрес.

### 9.3. Существующий PostgreSQL на другом сервере

- Создайте базу и пользователя (9.1) и откройте доступ только с адреса сервера бота (`pg_hba.conf` с `hostssl`, фаервол).
- Требуется постоянное соединение: убедитесь, что прокси или пулер между ботом и базой в режиме «сессия» либо отсутствует.
- Версия PostgreSQL – желательно 16 (на ней проходит набор тестов репозитория).
- Бэкапы и мониторинг – на стороне вашей базы; добавьте к ним копию каталога `media` (раздел 7.4).

## Что именно проверялось при подготовке документа

Среда: PostgreSQL 16.14 (локальный кластер на unix-сокете), база создана командой `python -m archive … migrate`, затем заполнена шестью синтетическими сообщениями (3 автора, одна правка, один ответ, одно голосовое с расшифровкой) через `Ingestor` из кода бота. Данных из реальных чатов не использовалось.

| Что | Результат |
|---|---|
| Запросы раздела 5 (период, топ авторов, имена, без ответа, поиск, правки, вложения, ответы, дни, размеры, `\copy` в CSV) | выполнены, ошибок нет, CSV создан |
| Удаление чата и человека (7.2, 7.3) | выполнены на копии базы, результат описан в тексте |
| Роль только для чтения (6.2) | создана и проверена, поведение описано в тексте |
| Схема и `search_path` для роли (9.1) | `migrate` дважды выполнен под ролью, таблицы только в схеме `oko` |
| `pg_dump -Fc`, `pg_restore --list`, восстановление и сверка (7.4) | выполнены локальными утилитами PostgreSQL 16 |
| `export`, `status`, `audit prune`, запросы `curl` к API | выполнены против локального API на той же базе |
| Команды `docker compose …`, образ `postgres` в контейнере, `docker compose exec` | **не запускались** (Docker в среде проверки недоступен) |
| Профиль памяти маленького сервера (раздел «Выбор»): `shared_buffers=64MB`, `work_mem=4MB`, `maintenance_work_mem=32MB`, `max_connections=20` и др. | отдельный кластер PostgreSQL 16 запущен с этими параметрами через `-c`; `SHOW` показал значения; `migrate`, загрузка данных и 8 параллельных запросов к API прошли без ошибок; расход памяти в контейнере не измерялся |
| Перенос малый → большой, вариант А (пустая база, `public`) и вариант Б (схема `oko`, переименование через временную базу, список `-L`) | оба выполнены локально: число строк и значение `events_id_seq` совпали, `migrate` напечатал `migration_ok`, `status` отработал |
| Подстановка переменных `PG_*` и `DB_MEM_LIMIT` в `compose.yaml` | **не запускалась** (нужен Docker); синтаксис `${VAR:-значение}` стандартный для Compose |
| Supabase | не проверялось, отмечено в тексте |
