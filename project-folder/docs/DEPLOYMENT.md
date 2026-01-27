# Deployment Automation Guide

This project now has three deployment automation tools for different environments:

## 📋 Quick Reference

| Tool | Platform | Use Case |
|------|----------|----------|
| `Makefile` | Linux/macOS/CI | Production CI/CD, Linux servers |
| `deploy.ps1` | Windows | Local development on Windows |
---

## 🪟 Windows Development (deploy.ps1)

### Full Deployment
```powershell
# Test → Build → Deploy
.\deploy.ps1

# Skip tests (faster development)
.\deploy.ps1 -SkipTests

# Clean everything first
.\deploy.ps1 -CleanFirst
```

### Partial Operations
```powershell
# Only run tests
.\deploy.ps1 -TestOnly

# Only build Docker images
.\deploy.ps1 -BuildOnly

# Skip build (just restart services)
.\deploy.ps1 -SkipBuild
```

### Combinations
```powershell
# Clean + Full deployment
.\deploy.ps1 -CleanFirst

# Quick restart (no test, no build)
.\deploy.ps1 -SkipTests -SkipBuild

# Verbose output
.\deploy.ps1 -Verbose
```

---

## 🐧 Linux/macOS (Makefile)

### Common Commands
```bash
# Show all available commands
make help

# Full deployment (test → build → deploy)
make deploy

# Run all tests
make test

# Run only unit tests (fast)
make test-unit

# Run only integration tests
make test-integration

# Build Docker images
make build

# Start development environment
make dev

# Clean everything
make clean
```

### Code Quality
```bash
# Check code quality (isort, black, flake8)
make lint

# Auto-format code
make format

# Quick check before committing
make pre-commit
```

### Other Utilities
```bash
# View logs
make logs

# Stop services
make stop

# Restart services
make restart

# Open shell in container
make shell
```
