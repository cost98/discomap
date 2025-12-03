# DiscoMap Database Reset Script
# Resetta completamente il database e reinizializza da zero

Write-Host "================================" -ForegroundColor Yellow
Write-Host "  DiscoMap Database Reset" -ForegroundColor Yellow
Write-Host "================================" -ForegroundColor Yellow
Write-Host ""
Write-Host "ATTENZIONE: Questo script cancellerà TUTTI i dati nel database!" -ForegroundColor Red
Write-Host ""

$confirm = Read-Host "Sei sicuro di voler continuare? (scrivi 'SI' per confermare)"

if ($confirm -ne "SI") {
    Write-Host "Reset annullato." -ForegroundColor Green
    exit 0
}

Write-Host ""
Write-Host "Step 1: Stopping containers..." -ForegroundColor Cyan
docker-compose down

Write-Host ""
Write-Host "Step 2: Removing postgres volume..." -ForegroundColor Cyan
docker volume rm docker_postgres-data 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "✓ Volume rimosso" -ForegroundColor Green
} else {
    Write-Host "⚠ Volume non trovato o già rimosso" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "Step 3: Removing pgadmin volume..." -ForegroundColor Cyan
docker volume rm docker_pgadmin-data 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "✓ Volume rimosso" -ForegroundColor Green
} else {
    Write-Host "⚠ Volume non trovato o già rimosso" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "Step 4: Starting postgres with fresh initialization..." -ForegroundColor Cyan
docker-compose up -d postgres

Write-Host ""
Write-Host "Step 5: Waiting for database to be ready..." -ForegroundColor Cyan
Start-Sleep -Seconds 10

$retries = 0
$maxRetries = 30
$ready = $false

while ($retries -lt $maxRetries -and -not $ready) {
    $result = docker exec discomap-postgres pg_isready -U discomap -d discomap 2>$null
    if ($LASTEXITCODE -eq 0) {
        $ready = $true
        Write-Host "✓ Database pronto!" -ForegroundColor Green
    } else {
        Write-Host "." -NoNewline
        Start-Sleep -Seconds 2
        $retries++
    }
}

if (-not $ready) {
    Write-Host ""
    Write-Host "✗ Timeout: Database non risponde" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "Step 6: Verifying schema..." -ForegroundColor Cyan
docker exec discomap-postgres psql -U discomap -d discomap -c "SELECT schemaname, tablename FROM pg_tables WHERE schemaname = 'airquality' ORDER BY tablename;"

Write-Host ""
Write-Host "================================" -ForegroundColor Green
Write-Host "  Reset Completato!" -ForegroundColor Green
Write-Host "================================" -ForegroundColor Green
Write-Host ""
Write-Host "Database inizializzato con schema pulito:" -ForegroundColor White
Write-Host "  • Tabella stations (stazioni fisiche)" -ForegroundColor Gray
Write-Host "  • Tabella sampling_points (sensori/strumenti)" -ForegroundColor Gray
Write-Host "  • Tabella measurements (time-series)" -ForegroundColor Gray
Write-Host "  • TimescaleDB hypertables configurato" -ForegroundColor Gray
Write-Host "  • Continuous aggregates attivi" -ForegroundColor Gray
Write-Host ""
Write-Host "Prossimi passi:" -ForegroundColor Cyan
Write-Host "  1. Avvia gli altri servizi: docker-compose up -d" -ForegroundColor White
Write-Host "  2. Importa metadati stazioni (CSV)" -ForegroundColor White
Write-Host "  3. Avvia sync dati: curl -X POST http://localhost:8000/sync/start ..." -ForegroundColor White
Write-Host ""
