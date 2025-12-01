# sqlglot-age-transform

A small Python package providing a custom **SQLGlot** transformation that rewrites the PostgreSQL-style `AGE()` function into an equivalent **Spark SQL** expression.

The project includes:

- An SQLGlot transformation implementation in `src/sqlglot_age_transform/transform.py`
- Basic tests under `tests/`

---

## 🚀 Prerequisites

This project uses **uv**, a modern Python package manager and workflow tool. [How to install uv?](https://docs.astral.sh/uv/getting-started/installation/)

Requires **Python 3.12+**.

---

## 📁 Setup
1. Fork this repository, then clone the fork
```shell
git clone https://github.com/your-username/sqlglot-age-transform
cd sqlglot-age-transform
````
2. Install all dependencies
```shell
uv sync --all-extras
```

3. Activate pre-commit hooks
```shell
uv run pre-commit install
```

## 🧹 Linting & Formatting
Check (lint only):
```shell
uv run ruff check
```

Auto-fix lint issues where supported:
```shell
uv run ruff check --fix
```

Format code:
```shell
uv run ruff format
```

## 🧪 Tests

Run tests:
```shell
uv run pytest
```
