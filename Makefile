.PHONY: dev clean cycle lint types test

dev:
	docker compose up -d

clean:
	docker compose down -v

cycle: clean dev

lint:
	uv run pre-commit run --all-files

types:
	uv run ty check -q

test:
	uv run pytest
