DOCKER_DESKTOP_BIN := /Applications/Docker.app/Contents/Resources/bin
ifneq ($(wildcard $(DOCKER_DESKTOP_BIN)/docker),)
export PATH := $(DOCKER_DESKTOP_BIN):$(PATH)
endif

.PHONY: up down build logs test lint demo

up:
	docker compose up --build -d

down:
	docker compose down --remove-orphans

build:
	docker compose build

logs:
	docker compose logs -f

test:
	python3 -m pytest

lint:
	ruff check .
	ruff format --check .

demo:
	./scripts/run_demo.sh

