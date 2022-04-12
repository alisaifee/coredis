lint:
	COREDIS_RUNTIME_CHECKS=0 python scripts/code_gen.py pipeline-stub
	black --check coredis tests
	pyright coredis
	mypy coredis
	flake8 coredis tests

lint-fix:
	COREDIS_RUNTIME_CHECKS=0 python scripts/code_gen.py pipeline-stub
	black coredis tests
	isort -r --profile=black tests coredis
	autoflake8 -i -r tests coredis

DEBUG := False
NEXT_VERSION := 3.5.0

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
