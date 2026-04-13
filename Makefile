VENV = .venv

.PHONY: help check_uv venv lint format check clean

help:
	@echo "Available commands:"
	@echo "  check_uv     - Check if uv is installed."
	@echo "  venv         - Create a virtual environment and install dependencies."
	@echo "  requirements - Install/sync dependencies from pyproject.toml."
	@echo "  lint         - Run linting checks."
	@echo "  format       - Format the code."
	@echo "  check        - Format the code and then run linting checks."
	@echo "  clean        - Remove virtual environment and cache files."
	@echo "  build        - Build Docker images."
	@echo "  run          - Run Docker containers in detached mode."
	@echo "  setup        - Build and run Docker containers."
	@echo "  logs         - Show logs from Docker containers."
	@echo "  stop         - Stop Docker containers."
	@echo "  destroy      - Destroy Docker containers."
	@echo "  clean-docker - Stop and destroy Docker containers."

# =============================================================================
# Development Environment Setup
# =============================================================================

check_uv:
	@if command -v uv >/dev/null 2>&1; then \
		echo "uv is installed."; \
	else \
		(echo "Error: uv is not installed. Please install it to continue."; \
		echo "Installation instructions: https://docs.astral.sh/uv/getting-started/installation/"; \
		exit 1); \
	fi

venv: check_uv
	@echo "Creating virtual environment with uv..."
	@uv venv
	@echo "Installing dependencies..."
	@uv pip install -e ".[dev]"
	@echo "Virtual environment created and dependencies installed."
	@echo "To activate, run: source .venv/bin/activate"

check_venv:
	@if [ ! -d "$(VENV)" ]; then \
		echo "Virtual environment not found. Please run 'make venv' first."; \
		exit 1; \
	fi

requirements: check_uv
	@uv sync

clean:
	@rm -rf $(VENV)
	@find . -type f -name "*.py[co]" -delete
	@find . -type d -name "__pycache__" -delete
	@find . -maxdepth 1 -type f -name "uv.lock" -delete
	@rm -rf .ruff_cache
	@echo "venv deleted, cache files removed"

# =============================================================================
# Code Quality
# =============================================================================

format: check_venv
	@echo "Formatting with ruff..."
	@.venv/bin/ruff format .
	@echo "Formatting with black..."
	@.venv/bin/black .
	@echo "Running ruff linter..."
	@.venv/bin/ruff check .

# =============================================================================
# Docker Commands
# =============================================================================

.PHONY: build run setup logs stop destroy clean-docker

build:
	@echo "Building Docker images..."
	@docker compose build

run:
	@echo "Running Docker containers in detached mode..."
	@docker compose up -d

setup:
	@echo "Building and running Docker containers in detached mode..."
	@docker compose up --build -d

logs:
	@echo "Showing logs..."
	@docker compose logs -f

stop:
	@echo "Stopping Docker containers..."
	@docker compose stop

destroy:
	@echo "Destroying Docker containers..."
	@docker compose down

clean-docker:
	@echo "Stopping and destroying Docker containers..."
	@docker compose down --volumes --remove-orphans
