# Brewery Data Pipeline - Makefile
# Automation for testing, building, and deploying

.PHONY: help test test-unit test-integration build deploy clean dev lint format

# Default target - show help
help:
	@echo "Brewery Data Pipeline - Available Commands:"
	@echo ""
	@echo "  make test              - Run all tests (unit + integration)"
	@echo "  make test-unit         - Run only unit tests (fast)"
	@echo "  make test-integration  - Run only integration tests in Docker"
	@echo "  make build             - Build Docker images"
	@echo "  make deploy            - Full deployment (test → build → up)"
	@echo "  make dev               - Start services for development"
	@echo "  make clean             - Clean up containers and artifacts"
	@echo "  make lint              - Run code quality checks"
	@echo "  make format            - Format code with black and isort"
	@echo ""

# Run all tests (unit + integration)
test: test-unit test-integration
	@echo "✓ All tests passed!"

# Run unit tests locally (fast, no Docker needed)
test-unit:
	@echo "Running unit tests..."
	pytest tests/unit/ -v --cov=src --cov-report=term --cov-report=html
	@echo "✓ Unit tests passed"

# Run integration tests in Docker
test-integration:
	@echo "Running integration tests in Docker..."
	@echo "Starting services..."
	docker-compose up -d
	@sleep 10
	@echo "Running tests..."
	docker-compose exec -T airflow-webserver pytest tests/integration/ -v
	@echo "Stopping services..."
	docker-compose down
	@echo "✓ Integration tests passed"

# Build Docker images
build:
	@echo "Building Docker images..."
	docker-compose build
	@echo "✓ Build complete"

# Full deployment: test → build → deploy
deploy: test build
	@echo "Deploying services..."
	docker-compose down
	docker-compose up -d
	@echo ""
	@echo "========================================="
	@echo "  ✓ Deployment Complete!"
	@echo "========================================="
	@echo ""
	@echo "Airflow UI: http://localhost:8080"
	@echo "Username: admin / Password: admin"
	@echo ""

# Start services for development
dev:
	@echo "Starting development environment..."
	docker-compose up

# Clean up everything
clean:
	@echo "Cleaning up..."
	docker-compose down -v
	rm -rf htmlcov/ .pytest_cache/ .coverage __pycache__/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	@echo "✓ Cleanup complete"

# Run linting checks
lint:
	@echo "Running code quality checks..."
	@echo "→ Checking imports with isort..."
	isort --check-only --diff src/ tests/
	@echo "→ Checking formatting with black..."
	black --check src/ tests/
	@echo "→ Running flake8..."
	flake8 src/ tests/ --max-line-length=120
	@echo "✓ Code quality checks passed"

# Format code automatically
format:
	@echo "Formatting code..."
	isort src/ tests/
	black src/ tests/
	@echo "✓ Code formatted"

# Quick check before commit (lint + test-unit)
pre-commit: lint test-unit
	@echo "✓ Ready to commit!"

# Show Docker logs
logs:
	docker-compose logs -f

# Stop all services
stop:
	docker-compose down

# Restart services
restart:
	docker-compose restart

# Open shell in webserver container
shell:
	docker-compose exec airflow-webserver bash
