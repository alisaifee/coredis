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
NEXT_VERSION := 4.12.0

coverage-docs:
	rm -rf docs/source/compatibility.rst
	PYTHONPATH=${CURDIR} python scripts/code_gen.py --debug=${DEBUG} --next-version=${NEXT_VERSION} coverage-doc

templated-sources:
	PYTHONPATH=${CURDIR} python scripts/code_gen.py token-enum
	PYTHONPATH=${CURDIR} python scripts/code_gen.py command-constants
	PYTHONPATH=${CURDIR} python scripts/code_gen.py cluster-key-extraction
	PYTHONPATH=${CURDIR} python scripts/code_gen.py pipeline-stub

benchmark:
	./scripts/benchmark.sh
benchmark-light:
	./scripts/benchmark.sh --data-size=10 --data-size=1000
benchmark-self:
	./scripts/benchmark.sh --data-size=10 --data-size=1000 -m coredis
