# Setup Hadoop winutils for PySpark on Windows
# Run this script once to configure Windows environment for local PySpark testing

Write-Host "Setting up Hadoop for PySpark on Windows..." -ForegroundColor Cyan
Write-Host ""

# 1. Create Hadoop directory
$hadoopHome = "C:\hadoop"
$hadoopBin = "$hadoopHome\bin"

if (Test-Path $hadoopBin) {
    Write-Host "[OK] Hadoop directory already exists: $hadoopBin" -ForegroundColor Green
} else {
    Write-Host "Creating Hadoop directory: $hadoopBin" -ForegroundColor Yellow
    New-Item -ItemType Directory -Force -Path $hadoopBin | Out-Null
    Write-Host "[OK] Created" -ForegroundColor Green
}

# 2. Download winutils.exe if not present
$winutilsPath = "$hadoopBin\winutils.exe"
if (Test-Path $winutilsPath) {
    Write-Host "[OK] winutils.exe already exists" -ForegroundColor Green
} else {
    Write-Host "Downloading winutils.exe..." -ForegroundColor Yellow
    try {
        $winutilsUrl = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/winutils.exe"
        curl.exe -L -o $winutilsPath $winutilsUrl 2>&1 | Out-Null
        Write-Host "[OK] Downloaded winutils.exe" -ForegroundColor Green
    } catch {
        Write-Host "[ERROR] Failed to download winutils.exe: $_" -ForegroundColor Red
        exit 1
    }
}

# 3. Download hadoop.dll if not present
$hadoopDllPath = "$hadoopBin\hadoop.dll"
if (Test-Path $hadoopDllPath) {
    Write-Host "[OK] hadoop.dll already exists" -ForegroundColor Green
} else {
    Write-Host "Downloading hadoop.dll..." -ForegroundColor Yellow
    try {
        $hadoopDllUrl = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/hadoop.dll"
        curl.exe -L -o $hadoopDllPath $hadoopDllUrl 2>&1 | Out-Null
        Write-Host "[OK] Downloaded hadoop.dll" -ForegroundColor Green
    } catch {
        Write-Host "[ERROR] Failed to download hadoop.dll: $_" -ForegroundColor Red
        exit 1
    }
}

# 4. Set HADOOP_HOME environment variable (User level - permanent)
Write-Host ""
Write-Host "Setting environment variables..." -ForegroundColor Yellow

$currentHadoopHome = [System.Environment]::GetEnvironmentVariable('HADOOP_HOME', [System.EnvironmentVariableTarget]::User)
if ($currentHadoopHome -eq $hadoopHome) {
    Write-Host "[OK] HADOOP_HOME already set correctly" -ForegroundColor Green
} else {
    [System.Environment]::SetEnvironmentVariable('HADOOP_HOME', $hadoopHome, [System.EnvironmentVariableTarget]::User)
    Write-Host "[OK] Set HADOOP_HOME=$hadoopHome (User environment)" -ForegroundColor Green
}

# 5. Add C:\hadoop\bin to PATH if not already there
$currentPath = [System.Environment]::GetEnvironmentVariable('Path', [System.EnvironmentVariableTarget]::User)
if ($currentPath -like "*$hadoopBin*") {
    Write-Host "[OK] PATH already contains $hadoopBin" -ForegroundColor Green
} else {
    $newPath = "$currentPath;$hadoopBin"
    [System.Environment]::SetEnvironmentVariable('Path', $newPath, [System.EnvironmentVariableTarget]::User)
    Write-Host "[OK] Added $hadoopBin to PATH (User environment)" -ForegroundColor Green
}

# 6. Set for current session
$env:HADOOP_HOME = $hadoopHome
$env:Path = "$env:Path;$hadoopBin"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Hadoop Setup Complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Environment variables set:" -ForegroundColor White
Write-Host "  HADOOP_HOME = $env:HADOOP_HOME" -ForegroundColor Gray
Write-Host "  PATH includes: $hadoopBin" -ForegroundColor Gray
Write-Host ""
Write-Host "IMPORTANT: Restart your terminal for permanent changes to take effect!" -ForegroundColor Yellow
Write-Host ""
Write-Host "To test PySpark locally, run:" -ForegroundColor White
Write-Host "  .\run_tests_local.ps1 -Verbose" -ForegroundColor Cyan
Write-Host ""
