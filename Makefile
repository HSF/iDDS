.PHONY: help install develop build clean wheel test lint format

help:
	@echo "iDDS Build Commands"
	@echo "==================="
	@echo ""
	@echo "  make install      - Install all packages"
	@echo "  make develop      - Install all packages in development mode"
	@echo "  make build        - Build all packages"
	@echo "  make wheel        - Build wheel distributions for all packages"
	@echo "  make clean        - Clean build artifacts"
	@echo "  make test         - Run tests for all packages"
	@echo "  make lint         - Run linting checks"
	@echo "  make format       - Format code with black"
	@echo ""

install:
	python build_all.py install

develop:
	python build_all.py develop

build:
	python build_all.py build

wheel:
	python build_all.py wheel

clean:
	python build_all.py clean
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type f -name "*.pyo" -delete 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true

test:
	pytest

lint:
	flake8 */lib/idds

format:
	black */lib/idds
