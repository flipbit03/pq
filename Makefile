.PHONY: dev clean cycle lint types test

dev:
	docker compose up -d

clean:
	docker compose down -v

cycle: clean dev

lint:
	uv run ruff check .
	uv run ruff format --check .

types:
	uv run ty check

test:
	uv run pytest
