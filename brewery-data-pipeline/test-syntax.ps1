param([switch]$Test)

$services = @(@{Name='test'; State='running'})

foreach ($service in $services) {
    $name = $service.Name
    $status = $service.State
    
    if ($status -eq "running") {
        Write-Host "✓" -ForegroundColor Green
    } else {
        Write-Host "✗" -ForegroundColor Red
    }
    
    Write-Host "$name - $status"
}

Write-Host "Done"
