.PHONY: dev clean cycle

dev:
	docker compose up -d

clean:
	docker compose down -v

cycle: clean dev
