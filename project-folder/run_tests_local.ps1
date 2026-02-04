# Run unit tests locally with proper Hadoop configuration
# This script sets HADOOP_HOME and runs pytest

param(
    [string]$TestPath = "tests/unit/",
    [switch]$Coverage,
    [switch]$Verbose
)

Write-Host "Running local unit tests..." -ForegroundColor Cyan
Write-Host ""

# Ensure HADOOP_HOME is set for PySpark
if (-not $env:HADOOP_HOME) {
    $env:HADOOP_HOME = 'C:\hadoop'
    Write-Host "[OK] Set HADOOP_HOME=$env:HADOOP_HOME for this session" -ForegroundColor Yellow
}

# Add hadoop native libraries to PATH (required for Delta Lake on Windows)
if ($env:PATH -notlike "*$env:HADOOP_HOME\bin*") {
    $env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"
    Write-Host "[OK] Added $env:HADOOP_HOME\bin to PATH for native libraries" -ForegroundColor Yellow
}

# Activate virtual environment
$venvPath = ".\bees-venv\Scripts\Activate.ps1"
if (Test-Path $venvPath) {
    & $venvPath
    Write-Host "[OK] Virtual environment activated" -ForegroundColor Green
    
    # Set PySpark Python to use same version for driver and workers
    $PythonPath = (Get-Command python).Source
    $env:PYSPARK_PYTHON = $PythonPath
    $env:PYSPARK_DRIVER_PYTHON = $PythonPath
    Write-Host "[OK] Set PYSPARK_PYTHON=$PythonPath" -ForegroundColor Green
} else {
    Write-Host "[ERROR] Virtual environment not found at $venvPath" -ForegroundColor Red
    Write-Host "  Run: py -3.11 -m venv bees-venv" -ForegroundColor Yellow
    exit 1
}

# Build pytest command
$pytestCmd = "pytest $TestPath"

if ($Verbose) {
    $pytestCmd += " -v"
}

if ($Coverage) {
    $pytestCmd += " --cov=src --cov-report=html --cov-report=term"
}

Write-Host ""
Write-Host "Running: $pytestCmd" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Gray
Write-Host ""

# Run pytest
Invoke-Expression $pytestCmd

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Gray
    Write-Host "[SUCCESS] All tests passed!" -ForegroundColor Green
    
    if ($Coverage) {
        Write-Host ""
        Write-Host "Coverage report generated: htmlcov/index.html" -ForegroundColor Cyan
    }
} else {
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Gray
    Write-Host "[FAILED] Some tests failed" -ForegroundColor Red
    exit 1
}
