# DiscoMap Docker Management Script (Windows PowerShell)
# Usage: .\docker-manage.ps1 [command] [options]

param(
    [Parameter(Position=0)]
    [string]$Command = "help",
    
    [Parameter(Position=1)]
    [string]$Option = ""
)

# Colors
function Write-Success { Write-Host "? $args" -ForegroundColor Green }
function Write-Error-Custom { Write-Host "? $args" -ForegroundColor Red }
function Write-Warning-Custom { Write-Host "??  $args" -ForegroundColor Yellow }
function Write-Info { Write-Host "[OK]?  $args" -ForegroundColor Cyan }
function Write-Header { 
    Write-Host "=================================================" -ForegroundColor Blue
    Write-Host "$args" -ForegroundColor Blue
    Write-Host "=================================================" -ForegroundColor Blue
}

# Command functions
function Build {
    Write-Header "Building DiscoMap Docker Image"
    docker-compose -f docker\docker-compose.yml -f docker\docker-compose.yml build
    Write-Success "Build completed"
}

function Start-Services {
    Write-Header "Starting DiscoMap Services"
    docker-compose -f docker\docker-compose.yml up -d
    Write-Success "Services started"
    Write-Host ""
    Status
}

function Stop-Services {
    Write-Header "Stopping DiscoMap Services"
    docker-compose -f docker\docker-compose.yml -f docker\docker-compose.yml down
    Write-Success "Services stopped"
}

function Restart {
    Write-Header "Restarting DiscoMap Services"
    docker-compose -f docker\docker-compose.yml -f docker\docker-compose.yml restart
    Write-Success "Services restarted"
}

function Status {
    Write-Header "DiscoMap Services Status"
    docker-compose -f docker\docker-compose.yml -f docker\docker-compose.yml ps
}

function Logs {
    param([string]$Service = "")
    
    if ($Service -eq "") {
        Write-Info "Showing logs for all services (Ctrl+C to exit)"
        docker-compose -f docker\docker-compose.yml logs -f --tail=100
    } else {
        Write-Info "Showing logs for $Service (Ctrl+C to exit)"
        docker-compose -f docker\docker-compose.yml logs -f --tail=100 $Service
    }
}

function Exec {
    param(
        [string]$Service = "sync-scheduler",
        [string]$Cmd = "bash"
    )
    
    Write-Info "Executing in $Service`: $Cmd"
    docker-compose -f docker\docker-compose.yml exec $Service $Cmd
}

function Shell {
    param([string]$Service = "sync-scheduler")
    
    Write-Info "Opening shell in $Service"
    docker-compose -f docker\docker-compose.yml exec $Service /bin/bash
}

function SyncNow {
    param([string]$Type = "incremental")
    
    Write-Header "Running $Type sync"
    docker-compose -f docker\docker-compose.yml exec sync-scheduler python src/sync_scheduler.py --$Type
}

function SyncTest {
    Write-Header "Running sync test"
    docker-compose -f docker\docker-compose.yml exec sync-scheduler python src/sync_scheduler.py --test
}

function SyncStatus {
    Write-Header "Sync Status"
    docker-compose -f docker\docker-compose.yml -f docker\docker-compose.yml exec sync-scheduler python src/sync_scheduler.py --status
}

function Clean {
    Write-Warning-Custom "This will remove all containers and volumes!"
    $response = Read-Host "Are you sure? (y/N)"
    
    if ($response -eq "y" -or $response -eq "Y") {
        Write-Header "Cleaning up DiscoMap"
        docker-compose -f docker\docker-compose.yml down -v
        Write-Success "Cleanup completed"
    } else {
        Write-Info "Cleanup cancelled"
    }
}

function CleanData {
    Write-Warning-Custom "This will delete all downloaded data!"
    $response = Read-Host "Are you sure? (y/N)"
    
    if ($response -eq "y" -or $response -eq "Y") {
        Write-Header "Cleaning data directory"
        Remove-Item -Path "data\raw\*.zip" -ErrorAction SilentlyContinue
        Remove-Item -Path "data\processed\*.parquet" -ErrorAction SilentlyContinue
        Write-Success "Data cleaned"
    } else {
        Write-Info "Data cleanup cancelled"
    }
}

function Update {
    Write-Header "Updating DiscoMap"
    git pull
    docker-compose -f docker\docker-compose.yml build
    docker-compose -f docker\docker-compose.yml up -d
    Write-Success "Update completed"
}

function Backup {
    $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $backupFile = "discomap_backup_$timestamp.zip"
    
    Write-Header "Creating backup: $backupFile"
    
    # Create temp directory for backup
    $tempDir = "temp_backup_$timestamp"
    New-Item -ItemType Directory -Path $tempDir -Force | Out-Null
    
    # Copy data (excluding large files)
    Copy-Item -Path "data" -Destination "$tempDir\data" -Recurse -Exclude "*.zip","*.log"
    Copy-Item -Path "config" -Destination "$tempDir\config" -Recurse -ErrorAction SilentlyContinue
    
    # Create zip
    Compress-Archive -Path "$tempDir\*" -DestinationPath $backupFile
    
    # Cleanup
    Remove-Item -Path $tempDir -Recurse -Force
    
    Write-Success "Backup created: $backupFile"
}

function ShowHelp {
    @"
DiscoMap Docker Management Script

Usage: .\docker-manage.ps1 [command] [options]

Commands:
  build                 Build Docker images
  start                 Start all services
  stop                  Stop all services
  restart               Restart all services
  status                Show services status
  logs [service]        Show logs (all services or specific)
  exec [service] [cmd]  Execute command in container
  shell [service]       Open bash shell in container
  
Sync Commands:
  sync-now [type]       Run sync now (full/incremental/hourly)
  sync-test             Test sync without downloading
  sync-status           Show sync status
  
Maintenance:
  clean                 Remove all containers and volumes
  clean-data            Remove downloaded data files
  update                Update code and rebuild
  backup                Create backup of data and config
  
Examples:
  .\docker-manage.ps1 start
  .\docker-manage.ps1 logs sync-scheduler
  .\docker-manage.ps1 sync-now incremental
  .\docker-manage.ps1 shell sync-scheduler

"@ | Write-Host
}

# Main command dispatcher
switch ($Command.ToLower()) {
    "build" { Build }
    "start" { Start-Services }
    "stop" { Stop-Services }
    "restart" { Restart }
    "status" { Status }
    "logs" { Logs -Service $Option }
    "exec" { Exec -Service $Option }
    "shell" { Shell -Service $Option }
    "sync-now" { SyncNow -Type $(if ($Option) { $Option } else { "incremental" }) }
    "sync-test" { SyncTest }
    "sync-status" { SyncStatus }
    "clean" { Clean }
    "clean-data" { CleanData }
    "update" { Update }
    "backup" { Backup }
    "help" { ShowHelp }
    default {
        Write-Error-Custom "Unknown command: $Command"
        Write-Host ""
        ShowHelp
        exit 1
    }
}


