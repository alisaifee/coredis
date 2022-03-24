#!/bin/bash
python -m venv .benchmarks
source .benchmarks/bin/activate
pip install -r python-redis-benchmark/requirements.txt
pip uninstall coredis -y
python setup.py develop
cd python-redis-benchmark
pytest $@
deactivate
cd ../
