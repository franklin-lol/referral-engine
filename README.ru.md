# referral-engine

[![Python](https://img.shields.io/badge/python-3.11%2B-blue)](https://www.python.org)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-passing-brightgreen)](#запуск-тестов)
[![CI](https://github.com/franklin-lol/referral-engine/actions/workflows/python-tests.yml/badge.svg)](https://github.com/franklin-lol/referral-engine/actions/workflows/python-tests.yml)

**Language / Язык:** [English](README.md) · [Русский](#)

---

Подключаемый, async-нативный движок многоуровневого реферального распределения для Python-сервисов.

Встраивается в любой бэкенд — Telegram-бот, REST API, gRPC-сервис — без изменения существующей схемы БД. Смените адаптер хранилища чтобы сменить базу данных. Настройте все параметры распределения через один YAML-файл.

---

## Какую проблему решает

Большинство реализаций реферальных систем жёстко вшиты в бизнес-логику: фиксированная глубина, нет идемпотентности, нет защиты от циклов, невозможно тестировать в изоляции. Эта библиотека чётко разделяет ответственности:

| Ответственность | Владелец |
|---|---|
| Алгоритм распределения | `ReferralEngine` |
| Хранилище и запросы | `BaseAdapter` (реализуете сами или используете встроенный) |
| Конфигурация | `config.yaml` (без перезапуска) |
| Ваше приложение | Вызывает движок, управляет транзакцией |

---

## Архитектура

```
┌─────────────────────────────────────────────────────────┐
│                   Ваше приложение                        │
│  (Telegram-бот / FastAPI / gRPC / фоновый воркер)       │
└────────────────────┬────────────────────────────────────┘
                     │  async with adapter:
                     │      await engine.distribute(...)
                     ▼
┌─────────────────────────────────────────────────────────┐
│                  ReferralEngine                          │
│  ┌──────────────┐  ┌───────────────┐  ┌─────────────┐  │
│  │  Distributor │  │  TreeTraversal│  │ CycleGuard  │  │
│  │  (алгоритм)  │  │ (вверх/вниз)  │  │ (DFS-check) │  │
│  └──────────────┘  └───────────────┘  └─────────────┘  │
└────────────────────┬────────────────────────────────────┘
                     │  реализует BaseAdapter
          ┌──────────┴──────────┐
          ▼                     ▼
┌─────────────────┐   ┌──────────────────┐
│ PostgresAdapter │   │  MemoryAdapter   │
│  (asyncpg)      │   │  (тесты / локал) │
└─────────────────┘   └──────────────────┘
          │
          ▼
┌─────────────────┐
│   PostgreSQL    │
│  re_users       │
│  re_referrals   │  ← closure-таблица (уровни 1..N)
│  re_accruals    │  ← неизменяемый аудит-лог
└─────────────────┘
```

---

## Ключевые возможности

**Настраиваемая глубина и ставки**  
До 50 уровней. Каждый уровень — своя ставка начисления. Меняйте ставки в YAML — без изменений кода и перезапуска.

**Идемпотентное распределение**  
Каждый вызов `distribute()` строит составной ключ из `(source_user_id, source_tag, дата)`. Повторный вызов с тем же событием не создаёт дублирующих начислений.

**Обнаружение циклов**  
Восходящий DFS при каждом назначении реферера. O(глубина), не O(размер дерева). Бросает `CycleDetectedError` до любой записи в БД.

**Фильтр активного депозита**  
При `require_active_deposit: true` предки без активного депозита пропускаются — обход продолжается вверх. Переключается через `update_deposit_status()`.

**Лимит дохода на событие**  
Опциональный максимум: ни один пользователь не получит больше `income_cap_per_event` за одно событие распределения, независимо от ставки.

**Пакетная обработка**  
`BatchDistributor` обрабатывает тысячи событий за запуск чанками — каждый чанк в отдельной транзакции. Создан для ежедневных cron-задач начисления прибыли.

**Система хуков**  
Регистрируйте async-колбэки для событий `accrual_created` / `distribution_complete` через `HookRegistry`. Ошибки в хуках никогда не влияют на результат распределения.

**Подключаемое хранилище**  
Реализуйте 8-методный интерфейс `BaseAdapter` для подключения любой БД. Встроенные адаптеры: PostgreSQL (`asyncpg`) и in-memory (для тестов).

**Нулевая связанность с ORM**  
Доменные модели — чистые Python-датаклассы. Никакого SQLAlchemy, Django ORM, никаких фреймворк-зависимостей в ядре библиотеки.

---

## Быстрый старт

```python
import asyncio
from decimal import Decimal
import asyncpg

from referral_engine import ReferralEngine, EngineConfig, PostgresAdapter

async def main():
    pool   = await asyncpg.create_pool("postgresql://user:pass@localhost/db")
    config = EngineConfig.from_yaml("config.yaml")

    adapter = PostgresAdapter(pool)
    engine  = ReferralEngine(config, adapter)

    # Регистрация пользователей
    async with adapter:
        alice = await engine.register_user("alice")
        bob   = await engine.register_user("bob",   referrer_id=alice.id)
        carol = await engine.register_user("carol", referrer_id=bob.id)

    # Отмечаем alice и bob как имеющих активный депозит
    async with adapter:
        await engine.update_deposit_status(alice.id, True)
        await engine.update_deposit_status(bob.id,   True)

    # Carol заработала 150 USDT прибыли → распределяем бонусы вверх
    async with adapter:
        result = await engine.distribute(
            source_user_id=carol.id,
            base_amount=Decimal("150.00"),
            source_tag="deposit_7_profit_20250624",
        )

    print(result.summary())
    # bob   → L1 → 12% → 18.00 USDT
    # alice → L2 → 10% → 15.00 USDT

asyncio.run(main())
```

Нет базы данных? Используйте in-memory адаптер:

```python
from referral_engine.adapters.memory import MemoryAdapter

adapter = MemoryAdapter()   # полная замена, тот же API
engine  = ReferralEngine(config, adapter)
```

---

## Конфигурация

Скопируйте `config.example.yaml` и настройте:

```yaml
# Ставки начисления по уровням (индекс 0 = уровень 1)
rates:
  - 12.0   # Уровень 1 — прямой реферер
  - 10.0   # Уровень 2
  -  8.0   # Уровень 3
  -  6.0   # Уровень 4
  -  5.0   # Уровень 5
  -  4.0   # Уровень 6
  -  3.0   # Уровень 7
  -  3.0   # Уровень 8
  -  2.0   # Уровень 9
  -  2.0   # Уровень 10
  -  1.0   # Уровни 11–15
  -  1.0
  -  1.0
  -  1.0
  -  1.0

max_depth: 50
require_active_deposit: true

# income_cap_per_event: 500.0   # опционально — раскомментируйте чтобы включить

min_accrual_amount: 0.000001
idempotency_window_seconds: 86400

leader_thresholds:
  - { level: 1, volume:   10000, bonus:   100 }
  - { level: 2, volume:   30000, bonus:   300 }
  - { level: 3, volume:   50000, bonus:   500 }
  - { level: 4, volume:  100000, bonus:  1000 }
```

Загружается при старте — смена ставок без перезапуска:

```python
config = EngineConfig.from_yaml("config.yaml")  # YAML
config = EngineConfig.from_json("config.json")  # JSON
config = EngineConfig.from_dict(my_dict)         # dict
```

---

## Справочник API

### `ReferralEngine`

```python
engine = ReferralEngine(config: EngineConfig, adapter: BaseAdapter)
```

| Метод | Описание |
|---|---|
| `register_user(external_id, referrer_id?)` | Создать пользователя, опционально привязать к рефереру |
| `set_referrer(user_id, referrer_id)` | Изменить реферера (с проверкой цикла) |
| `update_deposit_status(user_id, bool)` | Переключить фильтр активного депозита |
| `distribute(source_user_id, base_amount, source_tag)` | Запустить распределение вверх |
| `get_tree_up(user_id, max_levels?)` | Плоский список предков |
| `get_tree_down(user_id, max_levels?)` | Вложенное дерево потомков |
| `rebuild_tree(scope_user_id?)` | Перестроить многоуровневую closure-таблицу |
| `get_user_accruals(user_id, since?, limit?)` | История начислений |
| `get_user_total_accrued(user_id)` | Сумма за всё время |

### `BaseAdapter` — реализуйте для подключения любой БД

```python
class MyAdapter(BaseAdapter):
    async def get_user(self, user_id: int) -> Optional[User]: ...
    async def create_user(self, external_id: str) -> User: ...
    async def get_parent(self, user_id: int) -> Optional[ReferralLink]: ...
    async def get_chain_up(self, user_id, max_depth) -> List[ReferralLink]: ...
    async def create_referral_link(self, user_id, referrer_id) -> ReferralLink: ...
    async def rebuild_tree(self, scope_user_id=None) -> int: ...
    async def accrual_exists(self, source_key, recipient_user_id, level) -> bool: ...
    async def save_accrual(self, accrual: AccrualRecord) -> AccrualRecord: ...
    # + управление транзакцией: begin / commit / rollback
```

### `BatchDistributor` — пакетная обработка

```python
from referral_engine.batch import BatchDistributor, BatchEvent

batch  = BatchDistributor(engine, chunk_size=500)
result = await batch.distribute_many(events, on_error="skip")
# result.ok / result.failed / result.total_distributed
```

### `HookRegistry` — колбэки после начисления

```python
from referral_engine.hooks import HookRegistry, HookedEngine

registry = HookRegistry()

@registry.on("accrual_created")
async def notify(event):
    await bot.send_message(
        event.accrual.recipient_user_id,
        f"💰 +{event.accrual.accrual_amount} USDT (уровень {event.accrual.level})"
    )

engine = HookedEngine(ReferralEngine(config, adapter), registry)
```

---

## Алгоритм распределения

```
distribute(source_user_id=4, base=200 USDT, tag="profit_day1")

Дерево: root(1) ─► A(2) ─► B(3) ─► source(4)
Депозиты: root=✓  A=✗  B=✓

Обход вверх от source(4):
  прыжок 1 → B(3)    ставка=12%  депозит=✓  сумма=24.00  ✓ начислено
  прыжок 2 → A(2)    ставка=10%  депозит=✗  ПРОПУСК (обход продолжается)
  прыжок 3 → root(1) ставка= 8%  депозит=✓  сумма=16.00  ✓ начислено (уровень 3)

Результат: всего=40.00 USDT, levels_reached=2, skipped=1
```

Ключевые свойства:
- **Пропуск ≠ остановка.** Предок без депозита обходится стороной; его предки всё равно оцениваются.
- **Ставка позиционная по уровню.** Ставка root — `rates[2]` (8%), потому что он достигнут на 3-м прыжке.
- **Идемпотентно.** Вызов с тем же `source_tag` в тот же день — no-op.
- **Защита от несуществующих.** `distribute()` бросает `UserNotFoundError` если `source_user_id` не найден.

---

## Схема базы данных

```sql
-- Пользователи
re_users (id, external_id, has_active_deposit, is_active, created_at)

-- Closure-таблица — прямые связи (level=1) + производные многоуровневые
re_referrals (user_id, referrer_id, level, created_at)
  UNIQUE (user_id, level)

-- Неизменяемый лог начислений
re_accruals (source_user_id, recipient_user_id, level,
             base_amount, accrual_rate, accrual_amount,
             source_key, source_tag, accrued_at)
  UNIQUE (source_key, recipient_user_id, level)  -- идемпотентность

-- Лидерские награды (единоразовые бонусы)
re_leader_awards (user_id, level, bonus, awarded_at)
  UNIQUE (user_id, level)
```

Все таблицы с префиксом `re_` — не конфликтуют с вашей схемой. Применить миграцию один раз:

```bash
psql $DATABASE_URL -f migrations/001_schema.sql
```

---

## Запуск через Docker

```bash
docker-compose up
```

Поднимает PostgreSQL (схема применяется автоматически) и FastAPI-демо на порту 8000.

**Эндпоинты API:**

```
POST   /api/v1/users                         Зарегистрировать пользователя
PATCH  /api/v1/users/{id}/deposit            Переключить статус депозита
GET    /api/v1/users/{id}/accruals           История начислений

POST   /api/v1/referrals                     Назначить реферера
GET    /api/v1/referrals/{id}/tree/up        Цепочка предков
GET    /api/v1/referrals/{id}/tree/down      Дерево потомков
POST   /api/v1/referrals/rebuild             Перестроить closure-таблицу

POST   /api/v1/distributions                 Распределить прибыль
GET    /config                               Текущая конфигурация движка
```

Интерактивная документация: [http://localhost:8000/docs](http://localhost:8000/docs)

---

## Запуск тестов

```bash
pip install ".[test]"
pytest tests/ -v
```

Все тесты используют `MemoryAdapter` — база данных и Docker не нужны.

```
tests/test_distributor.py::TestRegistration::test_create_root_user              PASSED
tests/test_distributor.py::TestRegistration::test_register_with_referrer        PASSED
tests/test_distributor.py::TestRegistration::test_referrer_not_found_raises     PASSED
tests/test_distributor.py::TestRegistration::test_cycle_detection_self          PASSED
tests/test_distributor.py::TestRegistration::test_cycle_detection_two_nodes     PASSED
tests/test_distributor.py::TestRegistration::test_cycle_detection_deep          PASSED
tests/test_distributor.py::TestRegistration::test_change_referrer               PASSED
tests/test_distributor.py::TestTreeRebuild::test_multi_level_links_after_rebuild PASSED
tests/test_distributor.py::TestDistribution::test_single_level_distribution     PASSED
tests/test_distributor.py::TestDistribution::test_multi_level_distribution      PASSED
tests/test_distributor.py::TestDistribution::test_active_deposit_skip           PASSED
tests/test_distributor.py::TestDistribution::test_income_cap_per_event          PASSED
tests/test_distributor.py::TestDistribution::test_min_accrual_threshold         PASSED
tests/test_distributor.py::TestIdempotency::test_same_tag_same_day_no_duplicate PASSED
tests/test_distributor.py::TestIdempotency::test_different_tags_produce_multiple_accruals PASSED
tests/test_distributor.py::TestAccrualQueries::test_get_user_accruals_returns_records PASSED
tests/test_distributor.py::TestAccrualQueries::test_get_user_total_accrued      PASSED
tests/test_distributor.py::TestConfig::test_from_dict                           PASSED
tests/test_distributor.py::TestConfig::test_get_rate_out_of_range               PASSED
tests/test_distributor.py::TestConfig::test_total_payout_rate                   PASSED
tests/test_distributor.py::TestConfig::test_invalid_rate_raises                 PASSED
tests/test_distributor.py::TestConfig::test_invalid_level_raises                PASSED
tests/test_extensions.py::TestBatch::test_batch_distributes_all_events          PASSED
tests/test_extensions.py::TestBatch::test_batch_skip_on_error                   PASSED
tests/test_extensions.py::TestBatch::test_batch_chunking                        PASSED
tests/test_extensions.py::TestHooks::test_accrual_created_hook_fires            PASSED
tests/test_extensions.py::TestHooks::test_distribution_complete_hook            PASSED
tests/test_extensions.py::TestHooks::test_distribution_empty_hook_when_no_referrer PASSED
tests/test_extensions.py::TestHooks::test_hook_exception_does_not_break_result  PASSED
tests/test_extensions.py::TestHooks::test_multiple_handlers_all_fire            PASSED
tests/test_extensions.py::TestAnalytics::test_structure_volume                  PASSED
tests/test_extensions.py::TestAnalytics::test_level_stats                       PASSED
```

---

## Локальное демо (без базы данных)

```bash
python examples/quickstart.py
```

```
[ДЕРЕВО] Зарегистрировано 5 пользователей в прямой цепочке
  root(1) → l1(2) → l2(3) → l3(4) → src(5)

[ДЕПОЗИТЫ] root, l1, l2: активны | l3: нет

[РАСПРЕДЕЛЕНИЕ]  base=500.00 USDT
  Уровней достигнуто : 3
  Уровней пропущено  : 1  (l3 без депозита)
  Итого выплачено    : 130.000000 USDT

  L2  user_id=4    ставка= 10.0%  сумма=50.000000
  L3  user_id=3    ставка=  8.0%  сумма=40.000000
  L4  user_id=2    ставка=  6.0%  сумма=30.000000
  L5  user_id=1    ставка=  5.0%  сумма=25.000000

[ИДЕМПОТЕНТНОСТЬ] Второй вызов с тем же source_tag:
  Уровней достигнуто : 0  (все пропущены как дубликаты)
  Итого выплачено    : 0
```

---

## Расширение

**Свой адаптер (например MongoDB)**

```python
from referral_engine.adapters.base import BaseAdapter

class MongoAdapter(BaseAdapter):
    def __init__(self, db): self._db = db

    async def get_parent(self, user_id):
        doc = await self._db.referrals.find_one(
            {"user_id": user_id, "level": 1}
        )
        return ReferralLink(**doc) if doc else None

    # … реализуйте оставшиеся 7 методов
```

**Пакетный запуск ежедневной прибыли**

```python
from referral_engine.batch import BatchDistributor, BatchEvent

events = [
    BatchEvent(dep.user_id, dep.daily_profit, f"dep_{dep.id}_profit_{today}")
    for dep in active_deposits
]

batch  = BatchDistributor(engine, chunk_size=500)
result = await batch.distribute_many(events, on_error="skip")
print(f"Обработано {result.ok} / {result.ok + result.failed}")
```

**Хук: уведомление в Telegram после начисления**

```python
from referral_engine.hooks import HookRegistry, HookedEngine

registry = HookRegistry()

@registry.on("accrual_created")
async def notify_telegram(event):
    await bot.send_message(
        event.accrual.recipient_user_id,
        f"💰 +{event.accrual.accrual_amount} USDT (уровень {event.accrual.level})"
    )

engine = HookedEngine(ReferralEngine(config, adapter), registry)
```

**Переопределение конфига в рантайме (A/B-тест)**

```python
vip_config = EngineConfig.from_dict({"rates": [20.0, 15.0, 10.0]})
vip_engine  = ReferralEngine(vip_config, adapter)  # тот же адаптер, другие ставки
```

---

## Структура проекта

```
referral-engine/
│
├── referral_engine/               # ядро библиотеки — нулевые фреймворк-зависимости
│   ├── __init__.py                # публичная поверхность (ReferralEngine, EngineConfig, адаптеры, исключения)
│   ├── config.py                  # EngineConfig, LevelConfig, LeaderThreshold — загрузчики YAML/JSON/dict
│   ├── models.py                  # User, ReferralLink, TreeNode, AccrualRecord, DistributionResult
│   ├── exceptions.py              # CycleDetectedError, UserNotFoundError, DuplicateReferralError, ...
│   ├── tree.py                    # обнаружение циклов (DFS вверх), get_tree_up, get_tree_down
│   ├── distributor.py             # алгоритм распределения — идемпотентность, лимит, фильтр депозита
│   ├── engine.py                  # ReferralEngine — публичный API, связывает config + adapter + distributor
│   ├── batch.py                   # BatchDistributor — чанкованная пакетная обработка событий
│   ├── hooks.py                   # HookRegistry + HookedEngine — колбэки после начисления
│   ├── analytics.py               # Analytics — статистика по уровням, объём структуры
│   └── adapters/
│       ├── __init__.py
│       ├── base.py                # BaseAdapter — абстрактный интерфейс, 8 методов + транзакции
│       ├── postgres.py            # PostgresAdapter — asyncpg, рекурсивный CTE для rebuild_tree
│       └── memory.py              # MemoryAdapter — чистый Python, без БД, используется во всех тестах
│
├── api/                           # FastAPI-демо — показывает как интегрировать библиотеку
│   ├── __init__.py
│   ├── main.py                    # lifespan (инит пула), фабрика приложения, CORS, /health, /config
│   ├── state.py                   # общее изменяемое состояние приложения (pool, config) — инициализируется при старте
│   ├── deps.py                    # dependency injection FastAPI — get_adapter(), get_engine()
│   ├── core/
│   │   ├── __init__.py
│   │   └── config.py              # Settings через pydantic-settings — DATABASE_URL, ENGINE_CONFIG
│   └── v1/
│       ├── __init__.py
│       ├── router.py              # монтирует роутеры users / referrals / distributions
│       ├── users.py               # POST /users, PATCH /users/{id}/deposit, GET /users/{id}/accruals
│       ├── referrals.py           # POST /referrals, GET /tree/up|down, POST /rebuild
│       └── distributions.py      # POST /distributions
│
├── migrations/
│   └── 001_schema.sql             # re_users, re_referrals, re_accruals, re_leader_awards + индексы
│
├── tests/
│   ├── __init__.py
│   ├── test_distributor.py        # 22 теста — регистрация, дерево, распределение, идемпотентность, конфиг
│   └── test_extensions.py        # 11 тестов — batch, хуки, устойчивость хуков, аналитика
│
├── examples/
│   └── quickstart.py              # запускаемое демо, MemoryAdapter, без БД и Docker
│
├── .github/
│   └── workflows/
│       └── tests.yml              # CI — запускает все тесты при push/PR
│
├── .gitignore
├── CHANGELOG.md                   # история версий
├── CONTRIBUTING.md                # руководство по вкладу
├── LICENSE                        # MIT
├── SECURITY.md                    # отчёт об уязвимостях
├── config.example.yaml            # полностью документированный шаблон конфигурации
├── docker-compose.yml             # postgres (схема применяется автоматически) + api на :8000
├── Dockerfile
├── pyproject.toml                 # зависимости, опциональные группы: [api] [test] [dev], конфиг pytest
├── README.md                      # документация на английском
└── README.ru.md                   # документация на русском
```

---

## Вклад в проект

Смотрите [CONTRIBUTING.md](CONTRIBUTING.md).

---

## Лицензия

MIT — смотрите [LICENSE](LICENSE).
