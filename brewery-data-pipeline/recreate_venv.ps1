# Recreate Virtual Environment with Python 3.11
# Run this script to recreate bees-venv with the correct Python version

Write-Host "Recreating virtual environment with Python 3.11.9..." -ForegroundColor Cyan
Write-Host ""

# Check if Python 3.11 is available
try {
    $pyVersion = py -3.11 --version 2>&1
    Write-Host "[OK] Found Python: $pyVersion" -ForegroundColor Green
} catch {
    Write-Host "[ERROR] Python 3.11 not found!" -ForegroundColor Red
    Write-Host "  Please install Python 3.11.9 from https://www.python.org/downloads/" -ForegroundColor Yellow
    exit 1
}

# Check if bees-venv exists
if (Test-Path "bees-venv") {
    Write-Host "[WARNING] Virtual environment 'bees-venv' already exists" -ForegroundColor Yellow
    Write-Host "  Please close all terminals, VS Code, and running Python processes" -ForegroundColor Yellow
    Write-Host "  Then manually delete the 'bees-venv' folder and run this script again" -ForegroundColor Yellow
    Write-Host ""
    $response = Read-Host "Do you want to try deleting it now? (y/n)"
    if ($response -eq 'y') {
        try {
            Remove-Item -Recurse -Force bees-venv -ErrorAction Stop
            Write-Host "[OK] Deleted existing bees-venv" -ForegroundColor Green
        } catch {
            Write-Host "[ERROR] Could not delete bees-venv - files are in use" -ForegroundColor Red
            Write-Host "  Close VS Code and all terminals, then try again" -ForegroundColor Yellow
            exit 1
        }
    } else {
        Write-Host "Exiting. Please delete bees-venv manually and run this script again." -ForegroundColor Yellow
        exit 0
    }
}

# Create new virtual environment with Python 3.11
Write-Host "Creating new virtual environment..." -ForegroundColor Cyan
py -3.11 -m venv bees-venv

if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Failed to create virtual environment" -ForegroundColor Red
    exit 1
}

Write-Host "[OK] Virtual environment created" -ForegroundColor Green

# Activate virtual environment
Write-Host "Activating virtual environment..." -ForegroundColor Cyan
& ".\bees-venv\Scripts\Activate.ps1"

# Verify Python version
$activePython = python --version
Write-Host "[OK] Active Python: $activePython" -ForegroundColor Green

# Upgrade pip
Write-Host "Upgrading pip..." -ForegroundColor Cyan
python -m pip install --upgrade pip

# Install dependencies
Write-Host "Installing dependencies from requirements-local.txt..." -ForegroundColor Cyan
pip install -r requirements-local.txt

if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Failed to install dependencies" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "Virtual environment successfully created!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "Python version: $activePython" -ForegroundColor Cyan
Write-Host "Location: bees-venv\" -ForegroundColor Cyan
Write-Host ""
Write-Host "To activate in new terminals:" -ForegroundColor Yellow
Write-Host "  .\bees-venv\Scripts\Activate.ps1" -ForegroundColor White
Write-Host ""
Write-Host "To run tests:" -ForegroundColor Yellow
Write-Host "  .\run_tests_local.ps1 -Verbose" -ForegroundColor White
Write-Host ""
