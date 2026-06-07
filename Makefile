.PHONY: help install lint test test-property test-unit test-integration test-smoke clean

help:
	@echo "Available targets:"
	@echo "  install         Install the mcps package in editable mode with dev extras"
	@echo "  lint            Byte-compile the mcps package as a smoke check"
	@echo "  test            Run the full pytest suite"
	@echo "  test-property   Run only property-based tests (marker: property)"
	@echo "  test-unit       Run only unit tests"
	@echo "  test-integration  Run only integration tests"
	@echo "  test-smoke      Run only smoke tests"
	@echo "  clean           Remove build artefacts and caches"

install:
	pip install -e ".[dev]"

lint:
	python -m compileall mcps

test:
	pytest tests/

test-property:
	@pytest tests/ -m property; \
	rc=$$?; \
	if [ $$rc -eq 5 ]; then \
		echo "No property-based tests collected yet (exit 5 from pytest treated as success while the suite is empty)."; \
		exit 0; \
	fi; \
	exit $$rc

test-unit:
	pytest tests/unit/

test-integration:
	pytest tests/integration/

test-smoke:
	pytest tests/smoke/

clean:
	rm -rf build/ dist/ *.egg-info .pytest_cache .hypothesis
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
