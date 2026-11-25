.PHONY: install test demo up down

install:
	pip install -e ".[dev]"

test:
	pytest -q

demo:
	python -m examples.interview_pipeline.demo

up:
	docker compose up --build

down:
	docker compose down -v
