# 🐳 DiscoMap Docker Guide

Guida completa per l'utilizzo di DiscoMap con Docker e Docker Compose.

## 📋 Prerequisiti

- **Docker** 20.10+
- **Docker Compose** 2.0+
- **Git** (per clonare il repository)

### Installazione Docker

**Windows:**
- [Docker Desktop for Windows](https://docs.docker.com/desktop/install/windows-install/)

**Linux:**
```bash
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker $USER
```

**Mac:**
- [Docker Desktop for Mac](https://docs.docker.com/desktop/install/mac-install/)

## 🚀 Quick Start

### 1. Clone & Build

```bash
# Clone repository
git clone https://github.com/cost98/discomap.git
cd discomap

# Build images
docker-compose build

# Or use management script
chmod +x docker-manage.sh
./docker-manage.sh build
```

### 2. Start Services

```bash
# Start all services
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f
```

### 3. Run First Sync

```bash
# Test sync (no download)
docker-compose exec sync-scheduler python scripts/sync_scheduler.py --test

# Run incremental sync
docker-compose exec sync-scheduler python scripts/sync_scheduler.py --incremental

# Check status
docker-compose exec sync-scheduler python scripts/sync_scheduler.py --status
```

## 🎯 Architecture

### Services

#### 1. **sync-scheduler**
- **Purpose**: Main orchestrator per sync giornaliero
- **Schedule**: On-demand or manual trigger
- **Command**: `python scripts/sync_scheduler.py --incremental`
- **Restart**: `unless-stopped`

#### 2. **sync-hourly**
- **Purpose**: Sync automatico ogni ora
- **Schedule**: Loop infinito con sleep 3600s
- **Command**: Runs hourly sync in loop
- **Restart**: `unless-stopped`

#### 3. **duckdb-service** (optional)
- **Purpose**: Database query interface
- **Usage**: Interactive DuckDB console
- **Data**: Read-only access to processed data

### Volumes

```yaml
volumes:
  - ./data:/app/data          # Persist downloaded/processed data
  - ./logs:/app/logs          # Persist log files
  - ./config:/app/config:ro   # Mount configuration (read-only)
```

### Network

- **Network**: `discomap-net` (bridge)
- **Isolation**: Services can communicate internally
- **External**: No exposed ports by default (add dashboard for UI)

## 📖 Usage Guide

### Management Scripts

**Linux/Mac:**
```bash
chmod +x docker-manage.sh

# Show all commands
./docker-manage.sh help

# Start services
./docker-manage.sh start

# View logs
./docker-manage.sh logs sync-scheduler

# Run sync
./docker-manage.sh sync-now incremental

# Open shell
./docker-manage.sh shell sync-scheduler
```

**Windows PowerShell:**
```powershell
# Show all commands
.\docker-manage.ps1 help

# Start services
.\docker-manage.ps1 start

# View logs
.\docker-manage.ps1 logs sync-scheduler

# Run sync
.\docker-manage.ps1 sync-now incremental

# Open shell
.\docker-manage.ps1 shell sync-scheduler
```

### Direct Docker Compose Commands

```bash
# Build images
docker-compose build

# Start services (detached)
docker-compose up -d

# Stop services
docker-compose down

# Restart specific service
docker-compose restart sync-scheduler

# View logs
docker-compose logs -f sync-scheduler

# Execute command in container
docker-compose exec sync-scheduler python scripts/sync_scheduler.py --status

# Scale hourly sync (multiple instances)
docker-compose up -d --scale sync-hourly=2

# Remove everything including volumes
docker-compose down -v
```

### Sync Operations

```bash
# Test sync (no download)
docker-compose exec sync-scheduler python scripts/sync_scheduler.py --test

# Hourly sync (last 2 hours)
docker-compose exec sync-scheduler python scripts/sync_scheduler.py --hourly

# Incremental sync (since last run)
docker-compose exec sync-scheduler python scripts/sync_scheduler.py --incremental

# Full sync (last 7 days)
docker-compose exec sync-scheduler python scripts/sync_scheduler.py --full

# Check sync status
docker-compose exec sync-scheduler python scripts/sync_scheduler.py --status

# Custom sync
docker-compose exec sync-scheduler python scripts/sync_scheduler.py \
    --incremental --countries IT FR --pollutants NO2 PM10
```

## 🔧 Configuration

### Environment Variables

Create `.env` file:

```bash
# Application
APP_ENV=production
TZ=Europe/Rome

# Python
PYTHONUNBUFFERED=1
PYTHONDONTWRITEBYTECODE=1

# Sync Configuration
DEFAULT_COUNTRIES=IT,FR,DE
DEFAULT_POLLUTANTS=NO2,PM10,O3,SO2
LOOKBACK_DAYS=7
HOURLY_LOOKBACK_HOURS=2

# Logging
LOG_LEVEL=INFO
LOG_FORMAT=json

# Email notifications (optional)
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your-email@gmail.com
SMTP_PASSWORD=your-app-password
ALERT_EMAIL=admin@example.com
```

### Custom docker-compose.override.yml

```yaml
version: '3.8'

services:
  sync-scheduler:
    environment:
      - DEFAULT_COUNTRIES=IT,FR,DE,ES
      - LOOKBACK_DAYS=14
    volumes:
      - /path/to/external/storage:/app/data

  # Add dashboard service
  dashboard:
    build:
      context: .
      dockerfile: Dockerfile
    ports:
      - "8501:8501"
    command: streamlit run dashboard/app.py
    depends_on:
      - sync-scheduler
```

### Resource Limits

```yaml
services:
  sync-scheduler:
    deploy:
      resources:
        limits:
          cpus: '2.0'
          memory: 4G
        reservations:
          cpus: '1.0'
          memory: 2G
```

## 📊 Monitoring

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f sync-scheduler

# Last 100 lines
docker-compose logs --tail=100 sync-scheduler

# Follow with timestamps
docker-compose logs -f -t sync-scheduler
```

### Service Status

```bash
# Container status
docker-compose ps

# Resource usage
docker stats

# Service health
docker-compose ps | grep healthy
```

### Sync Status

```bash
# Inside container
docker-compose exec sync-scheduler python scripts/sync_scheduler.py --status

# From host (if sync_state.json is mounted)
cat data/sync_state.json | jq .
```

### Data Inspection

```bash
# List downloaded files
docker-compose exec sync-scheduler ls -lh data/raw/

# List processed files
docker-compose exec sync-scheduler ls -lh data/processed/

# Check disk usage
docker-compose exec sync-scheduler du -sh data/*

# View parquet file
docker-compose exec sync-scheduler python -c "
import pandas as pd
df = pd.read_parquet('data/processed/italy_clean.parquet')
print(df.head())
"
```

## 🐛 Troubleshooting

### Container Won't Start

```bash
# Check logs
docker-compose logs sync-scheduler

# Check container inspect
docker inspect discomap-sync

# Rebuild image
docker-compose build --no-cache sync-scheduler
docker-compose up -d
```

### Sync Fails

```bash
# Check sync logs
docker-compose logs sync-scheduler | grep ERROR

# Run test sync
docker-compose exec sync-scheduler python scripts/sync_scheduler.py --test

# Check network connectivity
docker-compose exec sync-scheduler curl -I https://eeadmz1-downloads-api-appservice.azurewebsites.net

# Verify Python dependencies
docker-compose exec sync-scheduler pip list
```

### Out of Disk Space

```bash
# Clean old data
./docker-manage.sh clean-data

# Remove unused Docker images
docker image prune -a

# Remove unused volumes
docker volume prune

# Check disk usage
df -h
docker system df
```

### Permission Issues

```bash
# Fix data directory permissions
sudo chown -R 1000:1000 data logs

# Or run as root (not recommended)
docker-compose exec -u root sync-scheduler bash
```

## 🚀 Production Deployment

### Docker Swarm

```bash
# Initialize swarm
docker swarm init

# Deploy stack
docker stack deploy -c docker-compose.yml discomap

# List services
docker service ls

# View service logs
docker service logs discomap_sync-scheduler

# Scale services
docker service scale discomap_sync-hourly=3

# Remove stack
docker stack rm discomap
```

### Kubernetes (k8s)

```bash
# Generate k8s manifests
kompose convert -f docker-compose.yml

# Deploy to k8s
kubectl apply -f discomap-deployment.yaml

# Check pods
kubectl get pods -l app=discomap

# View logs
kubectl logs -f deployment/discomap-sync

# Scale
kubectl scale deployment discomap-sync --replicas=3
```

### Cloud Deployment

**AWS ECS:**
- Use ECS CLI or CloudFormation
- Store images in ECR
- Use EFS for persistent volumes

**Azure Container Instances:**
- Deploy with Azure CLI
- Use Azure Files for storage
- Set up Azure Container Registry

**Google Cloud Run:**
- Deploy from Container Registry
- Use Cloud Storage for data
- Set up Cloud Scheduler for sync triggers

## 🔒 Security Best Practices

1. **Non-root user**: Container runs as user `discomap` (UID 1000)
2. **Read-only configs**: Mount config as `:ro`
3. **No exposed ports**: Services internal only (add reverse proxy if needed)
4. **Secrets management**: Use Docker secrets or environment variables
5. **Image scanning**: Regular vulnerability scans

```bash
# Scan image for vulnerabilities
docker scan discomap:latest

# Use Docker secrets
echo "my-secret" | docker secret create db_password -
```

## 📦 Backup & Restore

### Backup

```bash
# Using management script
./docker-manage.sh backup

# Manual backup
docker-compose exec sync-scheduler tar -czf /app/data/backup.tar.gz data/processed

# Copy to host
docker cp discomap-sync:/app/data/backup.tar.gz ./backups/
```

### Restore

```bash
# Copy backup to container
docker cp ./backups/backup.tar.gz discomap-sync:/app/data/

# Restore
docker-compose exec sync-scheduler tar -xzf /app/data/backup.tar.gz -C /app/
```

## 🎯 Best Practices

✅ **DO:**
- Use `.dockerignore` to exclude unnecessary files
- Pin base image versions (`python:3.13-slim`)
- Use multi-stage builds for smaller images
- Mount volumes for persistent data
- Use health checks
- Set resource limits in production
- Regular image updates and rebuilds

❌ **DON'T:**
- Don't run as root in production
- Don't store secrets in images
- Don't expose unnecessary ports
- Don't ignore security updates
- Don't skip health checks

## 📚 References

- [Docker Documentation](https://docs.docker.com/)
- [Docker Compose Reference](https://docs.docker.com/compose/compose-file/)
- [DiscoMap Main README](../README.md)
- [Sync Guide](./SYNC_GUIDE.md)

---

**Last Updated**: 2025-11-23  
**Docker Version**: 20.10+  
**Compose Version**: 2.0+
