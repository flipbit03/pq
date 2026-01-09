.PHONY: dev clean cycle lint types test release-patch release-minor release-major

dev:
	docker compose up -d --wait

clean:
	docker compose down -v

cycle: clean dev

lint:
	uv run pre-commit run --all-files

types:
	uv run ty check -q

test:
	uv run pytest

release-patch:
	./scripts/bump patch --push

release-minor:
	./scripts/bump minor --push

release-major:
	./scripts/bump major --push
