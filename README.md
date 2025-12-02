# pitertools
Tools to process python iterators in parallel.

## map_parallel
Concurrently run computation on iterator, respecting backpressure.

## Roadmap
- Add automated testing, publishing
- Add more tests 
- Allow running on Process pool executor

bootstrap instructions:
```
[install uv]
uv venv
uv sync
uv run ruff check .
uv run mypy pitertools
uv run pytest tests
```
