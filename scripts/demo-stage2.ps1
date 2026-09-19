# FlowForge Stage 2 PowerShell Demo Script
param(
    [string]$ApiUrl = "http://localhost:8080"
)

$ErrorActionPreference = "Stop"

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host " FlowForge Stage 2 Reliability & Retry Demo" -ForegroundColor Cyan
Write-Host " API Target: $ApiUrl" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""

# 1. Health & Readiness check
Write-Host "--> Checking API readiness..." -ForegroundColor Yellow
$ready = Invoke-RestMethod -Uri "$ApiUrl/readyz" -Method Get
Write-Host "    Status: $($ready.status), Postgres: $($ready.checks.postgres), Redis: $($ready.checks.redis)" -ForegroundColor Green
Write-Host ""

# 2. Submit a Flaky Job (demonstrating retries and eventual success)
Write-Host "--> 1. Submitting 'flaky' task (fails 2x, succeeds on attempt 3)..." -ForegroundColor Yellow
$flakyBody = @{
    type = "flaky"
    payload = @{}
    max_attempts = 3
} | ConvertTo-Json

$flakyResp = Invoke-RestMethod -Uri "$ApiUrl/jobs" -Method Post -Body $flakyBody -ContentType "application/json"
$flakyId = $flakyResp.id
Write-Host "    Created Job ID: $flakyId (Initial Status: $($flakyResp.status))" -ForegroundColor Green
Write-Host "    Watching retry state transitions (PENDING -> QUEUED -> RUNNING -> RETRY_WAIT -> ... -> COMPLETED)..."

for ($i = 0; $i -lt 30; $i++) {
    Start-Sleep -Seconds 1
    $statusResp = Invoke-RestMethod -Uri "$ApiUrl/jobs/$flakyId" -Method Get
    Write-Host "    [$((Get-Date).ToString('HH:mm:ss'))] Status: $($statusResp.status) | Attempts: $($statusResp.attempt_count)/$($statusResp.max_attempts)"
    if ($statusResp.status -eq "COMPLETED") {
        Write-Host "    Flaky Job succeeded on attempt $($statusResp.attempt_count)!" -ForegroundColor Green
        Write-Host "    Result: $($statusResp.result | ConvertTo-Json -Compress)" -ForegroundColor Green
        break
    }
}
Write-Host ""

# 3. Submit an Always Fail Job (demonstrating retry exhaustion)
Write-Host "--> 2. Submitting 'always_fail' task (exhausts all 3 attempts)..." -ForegroundColor Yellow
$alwaysFailBody = @{
    type = "always_fail"
    payload = @{}
    max_attempts = 3
} | ConvertTo-Json

$afResp = Invoke-RestMethod -Uri "$ApiUrl/jobs" -Method Post -Body $alwaysFailBody -ContentType "application/json"
$afId = $afResp.id
Write-Host "    Created Job ID: $afId (Status: $($afResp.status))" -ForegroundColor Green
Write-Host "    Watching retries until final FAILED status..."

for ($i = 0; $i -lt 30; $i++) {
    Start-Sleep -Seconds 1
    $statusResp = Invoke-RestMethod -Uri "$ApiUrl/jobs/$afId" -Method Get
    Write-Host "    [$((Get-Date).ToString('HH:mm:ss'))] Status: $($statusResp.status) | Attempts: $($statusResp.attempt_count)/$($statusResp.max_attempts)"
    if ($statusResp.status -eq "FAILED") {
        Write-Host "    Job exhausted retries and transitioned to FAILED as expected." -ForegroundColor Red
        Write-Host "    Final Error: $($statusResp.error)" -ForegroundColor Red
        break
    }
}
Write-Host ""

# 4. Submit a Permanent Fail Job (demonstrating immediate non-retryable failure)
Write-Host "--> 3. Submitting 'permanent_fail' task (non-retryable fast-path)..." -ForegroundColor Yellow
$permFailBody = @{
    type = "permanent_fail"
    payload = @{}
    max_attempts = 5
} | ConvertTo-Json

$pfResp = Invoke-RestMethod -Uri "$ApiUrl/jobs" -Method Post -Body $permFailBody -ContentType "application/json"
$pfId = $pfResp.id
Write-Host "    Created Job ID: $pfId (Status: $($pfResp.status))" -ForegroundColor Green
Write-Host "    Watching for immediate FAILED status..."

for ($i = 0; $i -lt 15; $i++) {
    Start-Sleep -Seconds 1
    $statusResp = Invoke-RestMethod -Uri "$ApiUrl/jobs/$pfId" -Method Get
    Write-Host "    [$((Get-Date).ToString('HH:mm:ss'))] Status: $($statusResp.status) | Attempts: $($statusResp.attempt_count)"
    if ($statusResp.status -eq "FAILED") {
        Write-Host "    Job failed immediately on attempt $($statusResp.attempt_count) without retrying." -ForegroundColor Magenta
        Write-Host "    Error: $($statusResp.error)" -ForegroundColor Magenta
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
Write-Host " Stage 2 Demo completed successfully!" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
