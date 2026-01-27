# Brewery Data Pipeline

A scalable, resilient Data Lake implementation following the Medallion Architecture (Bronze → Silver → Gold) for ingesting and processing brewery data from the Open Brewery DB API.

## Architecture

This solution implements a production-ready data pipeline with:
- **Medallion Architecture**: Bronze (raw) → Silver (cleaned) → Gold (aggregated)
- **Orchestration**: Apache Airflow for scheduling and monitoring
- **Processing**: PySpark for scalable data transformations
- **Storage**: Delta Lake format with partitioning
- **Quality**: SOLID-compliant validators per layer (BronzeValidator, SilverValidator, GoldValidator)
- **Deployment**: Docker Compose with automated deployment scripts

## Quick Start

### Prerequisites
- Docker Desktop (with Docker Compose)
- 8GB RAM minimum
- 10GB free disk space
- PowerShell (Windows) or Make (Linux/macOS) for deployment automation

### Automated Deployment (Recommended)

Use the deployment automation scripts for a one-command setup:

**Windows:**
```powershell
# Full deployment with tests
.\deploy.ps1

# Skip tests for faster deployment
.\deploy.ps1 -SkipTests
```

**Linux/macOS:**
```bash
# Full deployment with tests
make deploy

# Or run steps individually
make test        # Run all tests
make build       # Build Docker images
make up          # Start services
```

**Deployment Options:**
- See [DEPLOYMENT.md](DEPLOYMENT.md) for detailed deployment guide
- Run `.\deploy.ps1 -CleanFirst` to clean environment before deploying
- Run `.\deploy.ps1 -TestOnly` to run tests without deployment

### Manual Setup

```bash
# 1. Clone the repository
git clone <repository-url>
cd project-folder

# 2. Set up environment variables
cp .env.example .env
# Edit .env if needed (defaults work for local deployment)

# 3. Build and start services
docker-compose build
docker-compose up -d

# 4. Initialize Airflow
docker-compose exec airflow-webserver airflow db init
docker-compose exec airflow-webserver airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# 5. Access Airflow UI
# Open http://localhost:8080
# Login: admin / admin

# 6. Enable and trigger the DAG
# In Airflow UI, toggle ON the "brewery_pipeline" DAG
# Click "Trigger DAG" to run immediately
```

### Run Tests

```bash
# Run all tests
docker-compose exec airflow-webserver pytest tests/ -v

# Run with coverage
docker-compose exec airflow-webserver pytest tests/ --cov=src --cov-report=html

# Run specific test file
docker-compose exec airflow-webserver pytest tests/unit/brewery/test_brewery_api.py -v
```

## Data Flow

```breweries/
  - Raw JSON files
  - Immutable append-only storage
  - Minimal validation
        ↓
Silver Layer (/data/silver/breweries/)
  - Delta Lake format
  - Partitioned by country/state
  - Deduplication, standardization, quality scoring
        ↓
Gold Layer (/data/gold/)
  - Delta Lake aggregations
  - breweries_by_type, breweries_by_location
  - breweries_by_type_location, data_quality_/gold/
  - Business aggregations
  - Brewery counts by type & location
  - Data quality metrics
```

## Project Structure

```
brewery-dbrewery/             # Airflow DAGs & configuration
│   ├── brewery_pipeline.py   # Main DAG definition
│   └── brewery_config.yaml   # Pipeline configuration
├── src/                      # Source code
│   ├── ingestion/            # BaseAPIClient (generic HTTP client)
│   ├── layers/               # BronzeLayer, SilverLayer, GoldLayer
│   ├── pipelines/brewery/    # BreweryAPIClient & brewery tasks
│   ├── quality/              # Validators (Protocol-based)
│   ├── monitoring/           # Metrics collection & alerts
│   └── utils/                # Config, logging, Spark session
├── tests/                    # Test suite (unit + integration)
├── data/                     # Data lake storage (bronze/silver/gold)
├── docs/                     # Documentation
├── deploy.ps1                # Windows deployment automation
├── Makefile                  # Linux/macOS deployment automation
└── docker-compose.yml        # Service orchestration files
```

## Pipeline Features

### Data Quality
- Schema validation at each layer (Bronze, Silver, Gold)
- Deduplication and consistency checks
- Data quality scoring (0-100 per record)
- SOLID-compliant validators: `BronzeValidator`, `SilverValidator`, `GoldValidator`

### Error Handling
- Automatic retries with exponential backoff
- Task-level error handling
- Data quarantine for invalid records
- Comprehensive logging

### Monitoring
- Airflow UI for pipeline monitoring
- Structured JSON logging
- Data quality dashboards
- Alert notifications

## Configuration

Configuration is managed via:
- **YAML**: `dags/brewery/brewery_config.yaml` (paths, thresholds, URLs)
- **Environment**: `.env` file for secrets (gitignored)
- **Override**: Environment variables override YAML settings

Key settings:
```yaml
# brewery_config.yaml
layers:
  bronze:
    path: "bronze/breweries/"
  silver:
    path: "silver/breweries/"
    partitions: ["country", "state"]
  gold:
    path: "gold/"

**Unit Tests** (run locally, no Docker needed):
- Mocked Spark operations for fast execution
- Test API client, layers, validators, utilities
- Run: `pytest tests/unit/ -v`

**Integration Tests** (run in Docker with real Spark):
- End-to-end pipeline validation
- Delta Lake operations, partitioning
- Run: `docker-compose exec airflow-webserver pytest tests/integration/ -v`
