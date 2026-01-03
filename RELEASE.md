# Release Process

## Quick Release

```bash
make release-patch   # 0.2.2 -> 0.2.3 (bug fixes)
make release-minor   # 0.2.2 -> 0.3.0 (new features)
make release-major   # 0.2.2 -> 1.0.0 (breaking changes)
```

This bumps the version, commits, tags, and pushes. CI handles the rest.

## Prerequisites

- Clean working tree (`git status` shows no changes)
- All tests passing (`uv run pytest`)
- On `main` branch

## What Happens

1. Version in `pyproject.toml` is updated
2. Commit created with message "Release X.Y.Z"
3. Annotated git tag created
4. Pushed to remote
5. CI automatically:
   - Runs linting, type checking, and tests
   - Builds the package
   - Creates GitHub Release with artifacts
   - Publishes to PyPI

## Manual Usage

For explicit version control:

```bash
./scripts/bump patch              # auto-increment patch
./scripts/bump minor              # auto-increment minor
./scripts/bump major              # auto-increment major
./scripts/bump 1.0.0              # explicit version
./scripts/bump 1.0.0 --push       # bump and push
./scripts/bump 1.0.0 --force      # override version check
```

## Version Format

Semantic versioning without `v` prefix:

- `1.0.0` - Major (breaking changes)
- `1.1.0` - Minor (new features)
- `1.1.1` - Patch (bug fixes)

## PyPI

Published as `python-pq`:

```bash
pip install python-pq
```

Import as `pq`:

```python
from pq import PQ
```
