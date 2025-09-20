lint:
	uv run ruff check --select I coredis tests
	uv run ruff check coredis tests
	uv run ruff format --check coredis tests
	uv run mypy coredis

lint-fix:
	uv run ruff check --select I --fix coredis tests
	uv run ruff check --fix coredis tests
	uv run ruff format coredis tests
	uv run mypy coredis

DEBUG := False

coverage-docs:
	rm -rf docs/source/compatibility.rst
	PYTHONPATH=${CURDIR} uv run python -m scripts.code_gen --debug=${DEBUG} coverage-doc

templated-sources:
	PYTHONPATH=${CURDIR} uv run python -m scripts.code_gen token-enum
	PYTHONPATH=${CURDIR} uv run python -m scripts.code_gen command-constants
	PYTHONPATH=${CURDIR} uv run python -m scripts.code_gen cluster-key-extraction

benchmark:
	./scripts/benchmark.sh
benchmark-light:
	./scripts/benchmark.sh --data-size=10 --data-size=1000
benchmark-self:
	./scripts/benchmark.sh --data-size=10 --data-size=1000 -m coredis
