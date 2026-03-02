# pq

Postgres-backed job queue for Python.

## Development Services

Start Postgres with:

```bash
make dev
```

To restart Postgres (stop, clear data, start fresh):

```bash
make cycle
```

## Project Management

This project uses **uv** for dependency and project management. All commands should be run via `uv run`:

```bash
uv run <command>
uv run python main.py
uv run pytest
```

## Python Version

Python **3.14** is required.

## Code Quality

### Pre-commit Hooks

Pre-commit is configured for linting and formatting. Run manually with:

```bash
uv run pre-commit run --all-files
```

Install hooks for automatic execution on commit:

```bash
uv run pre-commit install
```

### Type Checking

All code must be **fully typed**. Use **ty** (v0.0.8) for type checking:

```bash
uv run ty check
```

Do not use `Any` types. All function parameters and return types must have explicit annotations.

### Linting & Formatting

Ruff handles both linting and formatting:

```bash
uv run ruff check .
uv run ruff format .
```

## Testing

Postgres must be running before tests (`make dev`). Use **pytest** for all tests:

```bash
uv run pytest
```

## Database

### SQLAlchemy

Use **SQLAlchemy 2.0-style** syntax:
- Use `select()` instead of `session.query()`
- Use `session.execute()` with `select()`, `insert()`, `update()`, `delete()`
- Use `session.scalars()` for single-column results
- Use type annotations with `Mapped[]` for model attributes

## Code Conventions

### Pydantic

Use **Pydantic** models wherever possible:
- Configuration and settings: `pydantic-settings`
- Data validation and serialization: Pydantic `BaseModel`
- Database models can use SQLAlchemy, but use Pydantic for API boundaries

### Project Structure

- `pq/` - Main package
- `migrations/` - Alembic database migrations
- `main.py` - Entry point

## Releases & Versioning

Do **not** bump the version in PRs. Releases are handled by the maintainer on `main` via `make release-patch/minor/major` (see `RELEASE.md` for full process).

### Migration Column Ordering

When creating migrations, order columns as follows:
1. Primary key (`id`)
2. Foreign keys
3. Business/data columns (including `client_id`)
4. `created_at`
5. `modified_at` / other timestamps
