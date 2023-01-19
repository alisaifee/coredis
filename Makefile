lint:
	black --check coredis tests
	ruff coredis tests
	pyright coredis
	mypy coredis

lint-fix:
	black coredis tests
	isort -r --profile=black tests coredis
	ruff --fix coredis tests

DEBUG := False
NEXT_VERSION := 4.0.0

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
