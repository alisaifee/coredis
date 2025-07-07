lint:
	ruff check --select I coredis tests
	ruff check coredis tests
	ruff format --check coredis tests
	mypy coredis

lint-fix:
	ruff check --select I --fix coredis tests
	ruff check --fix coredis tests
	ruff format coredis tests
	mypy coredis

DEBUG := False

coverage-docs:
	rm -rf docs/source/compatibility.rst
	PYTHONPATH=${CURDIR} python -m scripts.code_gen --debug=${DEBUG} coverage-doc

templated-sources:
	PYTHONPATH=${CURDIR} python -m scripts.code_gen token-enum
	PYTHONPATH=${CURDIR} python -m scripts.code_gen command-constants
	PYTHONPATH=${CURDIR} python -m scripts.code_gen cluster-key-extraction

benchmark:
	./scripts/benchmark.sh
benchmark-light:
	./scripts/benchmark.sh --data-size=10 --data-size=1000
benchmark-self:
	./scripts/benchmark.sh --data-size=10 --data-size=1000 -m coredis
