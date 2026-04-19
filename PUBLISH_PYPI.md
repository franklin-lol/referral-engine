# Публикация на PyPI — пошаговая инструкция

PyPI — это официальный реестр Python-пакетов. После публикации любой разработчик сможет установить твою библиотеку командой `pip install referral-engine`. Это создаёт доверие и видимость.

---

## Шаг 1. Регистрация на PyPI

1. Зайди на https://pypi.org/account/register/
2. Зарегистрируйся (email: fxranxklin@proton.me)
3. Включи 2FA (обязательно — без этого не дадут публиковать)
4. Создай API-токен: Account Settings → API tokens → Add API token → Scope: Entire account

---

## Шаг 2. Установи инструменты сборки

```bash
pip install build twine
```

---

## Шаг 3. Собери пакет

В корне проекта (там где pyproject.toml):

```bash
python -m build
```

Появится папка `dist/` с двумя файлами:
- `referral_engine-0.1.0.tar.gz`
- `referral_engine-0.1.0-py3-none-any.whl`

---

## Шаг 4. Проверь пакет перед публикацией

```bash
twine check dist/*
```

Должно вывести: `PASSED`

---

## Шаг 5. Опубликуй

```bash
twine upload dist/*
```

Введи:
- Username: `__token__`
- Password: твой API-токен (начинается с `pypi-`)

---

## После публикации

Пакет появится на: https://pypi.org/project/referral-engine/

Обнови бейдж в README.md — он уже добавлен и заработает автоматически после публикации:
```
[![PyPI](https://img.shields.io/pypi/v/referral-engine)](https://pypi.org/project/referral-engine/)
```

---

## Обновление версии

При следующих релизах:
1. Поменяй `version = "0.1.1"` в `pyproject.toml`
2. Повтори шаги 3–5

---

## Автоматизация через GitHub Actions (опционально)

Добавь в `.github/workflows/publish.yml`:

```yaml
name: Publish to PyPI

on:
  push:
    tags:
      - "v*"

jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install build twine
      - run: python -m build
      - run: twine upload dist/*
        env:
          TWINE_USERNAME: __token__
          TWINE_PASSWORD: ${{ secrets.PYPI_API_TOKEN }}
```

Добавь `PYPI_API_TOKEN` в GitHub → Settings → Secrets.
После этого достаточно создать тег `git tag v0.1.1 && git push --tags` — публикация произойдёт автоматически.
