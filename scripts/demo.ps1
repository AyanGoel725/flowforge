# FlowForge Stage 1 PowerShell Demo Script
param(
    [string]$ApiUrl = "http://localhost:8080"
)

$ErrorActionPreference = "Stop"

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host " FlowForge Stage 1 Live Demo" -ForegroundColor Cyan
Write-Host " API Target: $ApiUrl" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""

# 1. Health & Readiness check
Write-Host "--> Checking API readiness..." -ForegroundColor Yellow
$ready = Invoke-RestMethod -Uri "$ApiUrl/readyz" -Method Get
Write-Host "    Status: $($ready.status), Postgres: $($ready.postgres), Redis: $($ready.redis)" -ForegroundColor Green
Write-Host ""

# 2. Submit an Echo Job
Write-Host "--> 1. Submitting 'echo' job..." -ForegroundColor Yellow
$echoBody = @{
    type = "echo"
    payload = @{
        message = "Hello from FlowForge Stage 1 PowerShell Demo!"
    }
} | ConvertTo-Json

$echoResp = Invoke-RestMethod -Uri "$ApiUrl/jobs" -Method Post -Body $echoBody -ContentType "application/json"
$jobId = $echoResp.id
Write-Host "    Created Job ID: $jobId (Status: $($echoResp.status))" -ForegroundColor Green
Write-Host "    Polling until COMPLETED..."

for ($i = 0; $i -lt 20; $i++) {
    Start-Sleep -Seconds 1
    $statusResp = Invoke-RestMethod -Uri "$ApiUrl/jobs/$jobId" -Method Get
    Write-Host "    [$((Get-Date).ToString('HH:mm:ss'))] Status: $($statusResp.status)"
    if ($statusResp.status -eq "COMPLETED") {
        Write-Host "    Result: $($statusResp.result | ConvertTo-Json -Compress)" -ForegroundColor Green
        break
    }
}
Write-Host ""

# 3. Submit a Sleep Job (3 seconds)
Write-Host "--> 2. Submitting 'sleep' job (3s)..." -ForegroundColor Yellow
$sleepBody = @{
    type = "sleep"
    payload = @{
        seconds = 3
    }
} | ConvertTo-Json

$sleepResp = Invoke-RestMethod -Uri "$ApiUrl/jobs" -Method Post -Body $sleepBody -ContentType "application/json"
$sleepId = $sleepResp.id
Write-Host "    Created Job ID: $sleepId (Status: $($sleepResp.status))" -ForegroundColor Green
Write-Host "    Watching state transitions..."

for ($i = 0; $i -lt 20; $i++) {
    Start-Sleep -Seconds 1
    $statusResp = Invoke-RestMethod -Uri "$ApiUrl/jobs/$sleepId" -Method Get
    Write-Host "    [$((Get-Date).ToString('HH:mm:ss'))] Status: $($statusResp.status)"
    if ($statusResp.status -eq "COMPLETED") {
        Write-Host "    Final Result: $($statusResp.result | ConvertTo-Json -Compress)" -ForegroundColor Green
        break
    }
}
Write-Host ""

# 4. Submit an Invalid/Failing Job
Write-Host "--> 3. Submitting invalid task to demonstrate failure handling..." -ForegroundColor Yellow
$failBody = @{
    type = "nonexistent_task"
    payload = @{
        data = 123
    }
} | ConvertTo-Json

$failResp = Invoke-RestMethod -Uri "$ApiUrl/jobs" -Method Post -Body $failBody -ContentType "application/json"
$failId = $failResp.id
Write-Host "    Created Job ID: $failId (Status: $($failResp.status))" -ForegroundColor Green
Write-Host "    Watching for FAILED status..."

for ($i = 0; $i -lt 20; $i++) {
    Start-Sleep -Seconds 1
    $statusResp = Invoke-RestMethod -Uri "$ApiUrl/jobs/$failId" -Method Get
    Write-Host "    [$((Get-Date).ToString('HH:mm:ss'))] Status: $($statusResp.status)"
    if ($statusResp.status -eq "FAILED") {
        Write-Host "    Failure recorded: Error = '$($statusResp.error)'" -ForegroundColor Red
        break
    }
}
Write-Host ""

# 5. List all jobs
Write-Host "--> 4. Listing jobs..." -ForegroundColor Yellow
$listResp = Invoke-RestMethod -Uri "$ApiUrl/jobs?limit=5" -Method Get
Write-Host "    Total Jobs: $($listResp.total), Returned: $($listResp.jobs.Count)" -ForegroundColor Green
Write-Host ""

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host " Demo completed successfully!" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
