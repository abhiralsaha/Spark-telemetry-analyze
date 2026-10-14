PY ?= python3
.PHONY: install dev test lint typecheck cov demo docker clean zip
install:
	$(PY) -m pip install .
dev:
	$(PY) -m pip install -e ".[dev]"
test:
	PYTHONPATH=src $(PY) -m unittest discover -s tests -t . -v
lint:
	$(PY) -m ruff check src tests
typecheck:
	$(PY) -m mypy src
cov:
	$(PY) -m pytest --cov=spark_telemetry_analyzer --cov-report=term-missing
demo:
	mkdir -p examples/output
	PYTHONPATH=src $(PY) -m spark_telemetry_analyzer analyze examples/sample_mixed.json -f html -o examples/output/report.html
	PYTHONPATH=src $(PY) -m spark_telemetry_analyzer analyze examples/sample_mixed.json
docker:
	docker build -t spark-telemetry-analyzer:1.0.0 .
clean:
	rm -rf build dist *.egg-info .pytest_cache .mypy_cache .ruff_cache examples/output
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
