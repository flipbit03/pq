# Database

## Setup

Run migrations once at application startup:

```python
from pq import PQ

pq = PQ("postgresql://localhost/mydb")
pq.run_db_migrations()
```

This is safe to call multiple times - only pending migrations are applied.

## Tables

pq creates three tables with the `pq_` prefix:

| Table | Description |
|-------|-------------|
| `pq_tasks` | One-off tasks |
| `pq_periodic` | Periodic task schedules |
| `pq_schema_version` | Alembic migration tracking |

## Alembic Integration

If your application uses Alembic for migrations, exclude pq tables from autogenerate to avoid conflicts:

```python title="migrations/env.py"
EXCLUDED_TABLE_PREFIXES = ("pq_",)


def include_name(name: str | None, type_: str, parent_names: dict[str, str]) -> bool:
    """Filter out tables managed by external libraries."""
    if type_ == "table" and name is not None:
        return not name.startswith(EXCLUDED_TABLE_PREFIXES)
    return True


# In run_migrations_online():
context.configure(
    connection=connection,
    target_metadata=target_metadata,
    include_name=include_name,  # Add this
)
```

This prevents Alembic from generating migrations for pq tables, which are managed by `pq.run_db_migrations()`.
