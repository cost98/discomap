# DiscoMap Production Deployment Script (PowerShell)
# Usage: .\deploy.ps1 [version]
# Example: .\deploy.ps1 v1.0.0  (or omit for latest)

param(
    [string]$Version = "latest"
)

$ComposeFile = "docker-compose.prod.yml"
$EnvFile = ".env.production"

Write-Host "======================================" -ForegroundColor Cyan
Write-Host "DiscoMap Production Deployment" -ForegroundColor Cyan
Write-Host "======================================" -ForegroundColor Cyan
Write-Host "Version: $Version" -ForegroundColor Yellow
Write-Host ""

# Check if Docker is running
try {
    docker info | Out-Null
} catch {
    Write-Host "❌ Error: Docker is not running" -ForegroundColor Red
    exit 1
}

# Check if environment file exists
if (-not (Test-Path $EnvFile)) {
    Write-Host "❌ Error: $EnvFile not found" -ForegroundColor Red
    Write-Host "Create it from .env.production.example" -ForegroundColor Yellow
    exit 1
}

# Check for default passwords
$envContent = Get-Content $EnvFile -Raw
if ($envContent -match "changeme") {
    Write-Host "⚠️  Warning: Default passwords detected in $EnvFile" -ForegroundColor Yellow
    $response = Read-Host "Continue anyway? (y/N)"
    if ($response -ne "y" -and $response -ne "Y") {
        exit 1
    }
}

# Pull latest images
Write-Host "📦 Pulling Docker images (version: $Version)..." -ForegroundColor Green
if ($Version -eq "latest") {
    docker-compose -f $ComposeFile pull
} else {
    # Update image tags in compose file temporarily
    $composeContent = Get-Content $ComposeFile -Raw
    $composeContent = $composeContent -replace ":latest", ":$Version"
    $composeContent | Set-Content "$ComposeFile.tmp"
    docker-compose -f "$ComposeFile.tmp" pull
    Remove-Item "$ComposeFile.tmp"
}

# Stop existing containers
Write-Host "🛑 Stopping existing containers..." -ForegroundColor Yellow
docker-compose -f $ComposeFile down

# Start services
Write-Host "🚀 Starting services..." -ForegroundColor Green
docker-compose -f $ComposeFile --env-file $EnvFile up -d

# Wait for services to be healthy
Write-Host "⏳ Waiting for services to be healthy..." -ForegroundColor Cyan
Start-Sleep -Seconds 10

# Check status
Write-Host ""
Write-Host "📊 Service Status:" -ForegroundColor Cyan
docker-compose -f $ComposeFile ps

# Show logs
Write-Host ""
Write-Host "📝 Recent logs:" -ForegroundColor Cyan
docker-compose -f $ComposeFile logs --tail=20

Write-Host ""
Write-Host "✅ Deployment complete!" -ForegroundColor Green
Write-Host ""
Write-Host "Services available at:" -ForegroundColor Cyan
Write-Host "  - API: http://localhost:8000" -ForegroundColor White
Write-Host "  - Grafana: http://localhost:3000" -ForegroundColor White
Write-Host "  - pgAdmin: http://localhost:5050 (if tools profile enabled)" -ForegroundColor White
Write-Host ""
Write-Host "To view logs: docker-compose -f $ComposeFile logs -f" -ForegroundColor Yellow
Write-Host "To stop: docker-compose -f $ComposeFile down" -ForegroundColor Yellow
