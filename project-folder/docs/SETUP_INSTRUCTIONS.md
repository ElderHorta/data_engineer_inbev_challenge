# Setup Instructions

## Quick Verification

```powershell
cd project-folder
python test_setup.py
```

Checks Python version, packages, API connectivity, and Docker installation.

---

## 1. Local Development Setup (Unit Tests)

### Prerequisites
- **Python 3.11** (matches Docker environment)
- **Git**

### Step-by-Step

```powershell
# 1. Create and activate virtual environment
py -3.11.9 -m venv bees-venv
.\bees-venv\Scripts\Activate.ps1

# 2. Verify Python version
python --version

# 3. Install local dependencies (lightweight, ~2-3 min)
pip install -r requirements-local.txt
```

### Hadoop Setup (Required for Local PySpark)

PySpark on Windows requires `winutils.exe`. Run once:

```powershell
.\setup_hadoop_windows.ps1
```

Then **restart your terminal**.

**Verify:**
```powershell
echo $env:HADOOP_HOME  # Should show: C:\hadoop
```

### Running Local Tests

**Using the helper script (recommended):**
```powershell
# Run all unit tests
.\run_tests_local.ps1

# With coverage report
.\run_tests_local.ps1 -Coverage

# Specific test file
.\run_tests_local.ps1 -TestPath "tests/unit/layers/test_bronze_layer.py"
```

**Manual execution:**
```powershell
# Activate venv first
.\bees-venv\Scripts\Activate.ps1
$env:HADOOP_HOME = 'C:\hadoop'

# Run tests
pytest tests/unit/ -v

# With coverage
pytest tests/unit/ --cov=src --cov-report=html
```

---

## 2. Docker Setup (Full Pipeline)

### Prerequisites
- **Docker Desktop** (8GB RAM minimum)

### Step-by-Step

```powershell
# 1. Create environment file
copy .env.example .env

# 2. Deploy (builds, tests, starts)
.\deploy.ps1

# Or manual steps:
docker-compose build
docker-compose up -d
```

### Initialize Airflow (first time only)

```powershell
docker-compose exec airflow-webserver airflow db init
docker-compose exec airflow-webserver airflow users create `
    --username admin --password admin `
    --firstname Admin --lastname User `
    --role Admin --email admin@example.com
```

### Access Airflow UI
- **URL**: http://localhost:8080
- **Login**: admin / admin

### Docker Commands

```powershell
# View logs
docker-compose logs -f airflow-scheduler

# Run integration tests (requires real Spark)
docker-compose exec airflow-webserver pytest tests/integration/ -v

# Run all tests with coverage
docker-compose exec airflow-webserver pytest tests/ -v --cov=src

# Stop services
docker-compose down

# Clean restart
docker-compose down -v
docker-compose build --no-cache
docker-compose up -d
```

---

## 3. Requirements Files

| File | Use Case | Contents |
|------|----------|----------|
| `requirements-local.txt` | Local venv (bees-venv) | PySpark, pytest, linters (~150MB) |
| `requirements.txt` | Docker containers | Full stack + Airflow (~500MB) |

**Daily development**: Use `requirements-local.txt` in venv for fast unit tests.

---

## Common Issues

### Docker not found
Install Docker Desktop, restart PC, start Docker Desktop app.

### Port 8080 in use
```powershell
netstat -ano | findstr :8080
# Kill the process or change port in docker-compose.yml
```

### HADOOP_HOME not set
```powershell
.\setup_hadoop_windows.ps1
# Restart terminal
```

### Container memory issues
Docker Desktop → Settings → Resources → Increase Memory to 4GB+

### Module 'src' not found (Docker)
```powershell
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

---

## Development Workflow

1. **Edit code** in `src/` or `dags/`
2. **Run local tests**: `.\run_tests_local.ps1`
3. **Format code**: `black src/ tests/ && flake8 src/`
4. **Validate in Docker**: `docker-compose exec airflow-webserver pytest tests/ -v`
