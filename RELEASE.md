# Release Process

## Prerequisites

- Clean working tree (`git status` shows no changes)
- All tests passing (`uv run pytest`)
- On `main` branch

## Release Steps

1. **Bump version and create tag:**

   ```bash
   ./scripts/bump <version>
   ```

   This will:
   - Validate semver format (X.Y.Z)
   - Ensure new version > current version
   - Update `pyproject.toml`
   - Commit with message "Release X.Y.Z"
   - Create git tag `X.Y.Z`

2. **Push to trigger release:**

   ```bash
   git push && git push --tags
   ```

   Or do it in one step:

   ```bash
   ./scripts/bump <version> --push
   ```

3. **GitHub Actions will automatically:**
   - Run linting, type checking, and tests
   - Build the package
   - Create a GitHub Release with artifacts
   - Publish to PyPI (via trusted publishing)

## Version Format

Use semantic versioning without a `v` prefix:

- `1.0.0` - Major release
- `1.1.0` - Minor release (new features)
- `1.1.1` - Patch release (bug fixes)

## PyPI

The package is published as `python-pq` on PyPI:

```bash
pip install python-pq
```

Import remains `pq`:

```python
from pq import PQ
```
