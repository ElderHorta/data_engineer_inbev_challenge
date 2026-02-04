# Brewery Data Pipeline - Deploy Script
# Supports: Test -> Build -> Deploy with optional flags

param(
    [switch]$SkipTests,
    [switch]$SkipBuild,
    [switch]$TestOnly,
    [switch]$BuildOnly,
    [switch]$CleanFirst
)

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Brewery Pipeline - Deploy Script     " -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Step 0: Clean if requested
if ($CleanFirst) {
    Write-Host "[0/3] Cleaning environment..." -ForegroundColor Yellow
    docker-compose down -v 2>$null
    Write-Host "Done: Environment cleaned!" -ForegroundColor Green
    Write-Host ""
}

# Step 1: Run Tests (unless skipped)
if (-not $SkipTests -and -not $BuildOnly) {
    Write-Host "[1/3] Running tests..." -ForegroundColor Yellow

    # Activate virtual environment
    $venvPath = ".\bees-venv\Scripts\Activate.ps1"
    if (Test-Path $venvPath) {
        & $venvPath
    } else {
        Write-Host "Virtual environment not found. Run: py -3.11 -m venv bees-venv" -ForegroundColor Red
        exit 1
    }

    # Set PySpark environment
    if (-not $env:HADOOP_HOME) {
        $env:HADOOP_HOME = "C:\hadoop"
    }
    $env:PYSPARK_PYTHON = (Get-Command python).Source
    $env:PYSPARK_DRIVER_PYTHON = (Get-Command python).Source

    # Run tests
    pytest tests/unit/ -v --cov=src

    if ($LASTEXITCODE -ne 0) {
        Write-Host "Error: Tests failed! Fix them before deploying." -ForegroundColor Red
        exit 1
    }

    Write-Host "Done: Tests passed!" -ForegroundColor Green
    Write-Host ""

    if ($TestOnly) {
        Write-Host "========================================" -ForegroundColor Green
        Write-Host "  Done: Tests Complete (TestOnly mode) " -ForegroundColor Green
        Write-Host "========================================" -ForegroundColor Green
        exit 0
    }
} else {
    Write-Host "[1/3] Skipping tests..." -ForegroundColor Gray
}

# Step 2: Build Docker Images (unless skipped)
if (-not $SkipBuild -and -not $TestOnly) {
    Write-Host "[2/3] Building Docker images..." -ForegroundColor Yellow

    docker-compose build

    if ($LASTEXITCODE -ne 0) {
        Write-Host "Error: Build failed!" -ForegroundColor Red
        exit 1
    }

    Write-Host "Done: Build complete!" -ForegroundColor Green
    Write-Host ""

    if ($BuildOnly) {
        Write-Host "========================================" -ForegroundColor Green
        Write-Host "  Done: Build Complete (BuildOnly mode)" -ForegroundColor Green
        Write-Host "========================================" -ForegroundColor Green
        exit 0
    }
} else {
    Write-Host "[2/3] Skipping build..." -ForegroundColor Gray
}

# Step 3: Start Containers
if (-not $TestOnly -and -not $BuildOnly) {
    Write-Host "[3/3] Starting containers..." -ForegroundColor Yellow

    docker-compose down 2>$null
    docker-compose up -d

    if ($LASTEXITCODE -ne 0) {
        Write-Host "Error: Failed to start containers!" -ForegroundColor Red
        exit 1
    }

    Write-Host "Done: Containers started!" -ForegroundColor Green
    Write-Host ""

    # Wait for services
    Write-Host "Waiting for services to be ready..." -ForegroundColor Cyan
    Start-Sleep -Seconds 10

    # Show results
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Green
    Write-Host "  Done: Deployment Complete!           " -ForegroundColor Green
    Write-Host "========================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "Airflow Web UI: http://localhost:8080" -ForegroundColor Cyan
    Write-Host "Username: admin" -ForegroundColor Cyan
    Write-Host "Password: admin" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "View logs: docker-compose logs -f" -ForegroundColor Yellow
    Write-Host ""
}
