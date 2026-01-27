# Brewery Data Pipeline - Simple Deploy Script
# Run tests → Build Docker → Start containers

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Brewery Pipeline - Deploy Script     " -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Step 1: Run Tests
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
    $env:HADOOP_HOME = 'C:\hadoop'
}
$env:PYSPARK_PYTHON = (Get-Command python).Source
$env:PYSPARK_DRIVER_PYTHON = (Get-Command python).Source

# Run tests
pytest tests/unit/ -v --cov=src

if ($LASTEXITCODE -ne 0) {
    Write-Host "✗ Tests failed! Fix them before deploying." -ForegroundColor Red
    exit 1
}

Write-Host "✓ Tests passed!" -ForegroundColor Green
Write-Host ""

# Step 2: Build Docker Images
Write-Host "[2/3] Building Docker images..." -ForegroundColor Yellow

docker-compose build

if ($LASTEXITCODE -ne 0) {
    Write-Host "✗ Build failed!" -ForegroundColor Red
    exit 1
}

Write-Host "✓ Build complete!" -ForegroundColor Green
Write-Host ""

# Step 3: Start Containers
Write-Host "[3/3] Starting containers..." -ForegroundColor Yellow

docker-compose down 2>$null
docker-compose up -d

if ($LASTEXITCODE -ne 0) {
    Write-Host "✗ Failed to start containers!" -ForegroundColor Red
    exit 1
}

Write-Host "✓ Containers started!" -ForegroundColor Green
Write-Host ""

# Wait for services
Write-Host "Waiting for services to be ready..." -ForegroundColor Cyan
Start-Sleep -Seconds 10

# Show results
Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "  ✓ Deployment Complete!               " -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "Airflow Web UI: " -NoNewline
Write-Host "http://localhost:8080" -ForegroundColor Cyan
Write-Host "Username: " -NoNewline
Write-Host "admin" -ForegroundColor Cyan
Write-Host "Password: " -NoNewline
Write-Host "admin" -ForegroundColor Cyan
Write-Host ""
Write-Host "View logs: " -NoNewline
Write-Host "docker-compose logs -f" -ForegroundColor Yellow
Write-Host ""

