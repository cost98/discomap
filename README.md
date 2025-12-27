# ?? DiscoMap - EEA Air Quality Data Platform

PostgreSQL + TimescaleDB platform for downloading, storing, and analyzing air quality data from the European Environment Agency (EEA).

## ?? Features

- 🔄 **REST API** - FastAPI server for sync control and data operations
- 📊 **Time-Series Database** - PostgreSQL + TimescaleDB with automatic compression
- 🐳 **Docker Deployment** - Production-ready containerized stack
- ✅ **Data Validation** - Automatic quality checks and cleaning
- 📈 **Monitoring Tools** - PgAdmin and Grafana included
- ⚡ **Direct URL Sync** - Bypass EEA API limitations with direct Parquet URLs- 🗄️ **SQLAlchemy ORM** - Type-safe async database operations with repository pattern
- 🔄 **Alembic Migrations** - Version-controlled database schema management
## ?? Quick Start

### Prerequisites
- Docker 20.10+
- Docker Compose 2.0+

### Installation

```bash
# Clone repository
git clone https://github.com/cost98/discomap.git
cd discomap/.docker

# Configure environment (optional)
cp .env.example .env
# Edit .env with your passwords

# Start all services
docker compose up -d

# Check status
docker compose ps

# Check API health
curl http://localhost:8000/health
```



## ??? Database Schema

### Main Tables

- **`stations`** - Physical monitoring stations (location, metadata)
- **`sampling_points`** - Individual sensors/instruments at each station
- **`measurements`** - Time-series data (TimescaleDB hypertable, compressed after 7 days)
- **`pollutants`** - Pollutant codes and metadata (PM10, PM2.5, O3, NO2, etc.)
- **`countries`** - Country codes and names
- **`validity_flags`** - Data validation status codes
- **`verification_status`** - Data verification level codes
- **`sync_operations`** - Sync operation tracking and history

### Key Features

- **Automatic Compression** - 90% storage reduction for old data (7-day policy)
- **Normalized Structure** - Stations separated from sensors for better data integrity
- **Data Validation** - Automatic quality checks on insert
- **Sync Tracking** - Complete operation history with statistics

## ?? Monitoring

### PgAdmin (Database Management)

PgAdmin � opzionale e richiede configurazione manuale del server.

```bash
# Start PgAdmin (optional)
cd .docker
docker compose --profile tools up -d pgadmin

# Accedi: http://localhost:5050
# Email: admin@example.com
# Password: admin (cambia in .env con PGADMIN_EMAIL/PGADMIN_PASSWORD)
```

**Configurazione server PostgreSQL:**
1. In PgAdmin, clicca "Add New Server"
2. Tab "General": Name = `DiscoMap`
3. Tab "Connection":
   - Host: `postgres`
   - Port: `5432`
   - Database: `discomap`
   - Username: `discomap`
   - Password: `changeme`
   - ? Save password
4. Clicca "Save"

Le tabelle sono nello schema `airquality`.

### Grafana (Visualization)

Grafana si avvia automaticamente con tutti i servizi e include:
- ? Datasource PostgreSQL auto-configurato
- ? Dashboard "Air Quality - Real Time" pre-caricata

```bash
# Accedi a Grafana (gi� in esecuzione)
# URL: http://localhost:3000
# User: admin
# Password: admin (cambia in .env con GRAFANA_PASSWORD)

# Dashboard diretto: http://localhost:3000/d/aq-realtime-new
```

### Database Statistics

```bash
# Connect to database
docker exec -it discomap-postgres psql -U discomap -d discomap

# Run queries
SELECT * FROM airquality.measurements ORDER BY time DESC LIMIT 10;
SELECT COUNT(*) FROM airquality.measurements;
SELECT * FROM airquality.daily_stats WHERE date = CURRENT_DATE;
```

## ?? Sync Modes

### From URLs (Recommended)
Direct download from Parquet URLs, bypassing EEA API date limitations:

```powershell
# Get URLs for date range
$result = Invoke-RestMethod -Uri "http://localhost:8000/sync/get-urls?countries=IT&pollutants=PM10&start_date=2023-01-01&end_date=2023-01-31&dataset=2" -Method POST

# Start sync
$body = @{
    sync_type = "from_urls"
    parquet_urls = $result.urls
    max_workers = 8
} | ConvertTo-Json -Depth 10

Invoke-RestMethod -Uri "http://localhost:8000/sync/start" -Method POST -Headers @{"Content-Type"="application/json"} -Body $body
```

### Incremental (On-Demand)
Sync recent data via API:
```bash
curl -X POST http://localhost:8000/sync/start \
  -H "Content-Type: application/json" \
  -d '{"sync_type":"incremental","countries":["IT"],"pollutants":["PM10"],"max_workers":8}'
```

### Custom Period
Specify exact date range:
```bash
curl -X POST http://localhost:8000/sync/start \
  -H "Content-Type: application/json" \
  -d '{"sync_type":"custom_period","countries":["IT","FR"],"pollutants":["PM10","NO2"],"start_date":"2023-01-01","end_date":"2023-12-31","max_workers":8}'
```

## ??? Management Commands

### Docker Compose Commands

```bash
# Navigate to docker directory
cd .docker

# Start all services
docker compose up -d

# Stop all services
docker compose down

# View logs
docker compose logs -f api
docker compose logs -f postgres

# Restart service
docker compose restart api

# Rebuild and restart
docker compose up -d --build api
```

### Using Management Scripts

**Linux/Mac:**
```bash
chmod +x docker-manage.sh
./docker-manage.sh start
./docker-manage.sh sync-now incremental
./docker-manage.sh logs sync-scheduler
./docker-manage.sh backup
```

**Windows:**
```powershell
.\docker-manage.ps1 start
.\docker-manage.ps1 sync-now incremental
.\docker-manage.ps1 logs sync-scheduler
.\docker-manage.ps1 backup
```

## ?? Project Structure

```
discomap/
+-- docker/
�   +-- postgres/           # PostgreSQL initialization scripts
�       +-- init-db.sql
�       +-- create-tables.sql
�       +-- create-hypertables.sql
+-- src/                    # Python modules
�   +-- config.py
�   +-- downloader.py
�   +-- parser.py
�   +-- validators.py
�   +-- db_writer.py        # PostgreSQL writer
�   +-- ...
+-- scripts/
�   +-- sync_scheduler.py   # Main sync orchestrator
�   +-- download_eea_data.py
+-- tests/                  # Unit tests (pytest)
�   +-- test_config.py
�   +-- test_parser.py
�   +-- ...
+-- docs/
�   +-- DOCKER_GUIDE.md     # Complete Docker documentation
�   +-- SYNC_GUIDE.md       # Sync automation guide
+-- data/                   # Persistent data
�   +-- raw/                # Downloaded files
�   +-- processed/          # Processed data
+-- logs/                   # Application logs
+-- Dockerfile              # Python app container
+-- Dockerfile.postgres     # TimescaleDB container
+-- docker-compose.yml      # Service orchestration
+-- requirements.txt
+-- README.md
```

## 🏗️ Architecture

### Database Layer

**SQLAlchemy 2.0 + AsyncPG** for modern async operations:

```python
from src.database import get_db_session, StationRepository

async with get_db_session() as session:
    station_repo = StationRepository(session)
    station = await station_repo.get_by_code("IT0508A")
```

**Key Benefits:**
- ⚡ **3x faster** than psycopg2 (AsyncPG driver)
- 🔒 **Type-safe** with SQLAlchemy 2.0 `Mapped[T]` syntax
- 🔄 **Versioned migrations** with Alembic
- 🧩 **Repository pattern** for clean separation of concerns
- 📦 **Connection pooling** managed automatically

### Database Migrations

Use Alembic for schema changes:

```bash
# Generate migration from model changes
python -m alembic revision --autogenerate -m "description"

# Apply migrations
python -m alembic upgrade head

# Rollback one migration
python -m alembic downgrade -1

# View migration history
python -m alembic history
```

### Repository Pattern

Available repositories in `src.database.repositories`:
- `StationRepository` - Station CRUD operations
- `SamplingPointRepository` - Sampling point management
- `MeasurementRepository` - Time-series data (bulk inserts optimized)
- `PollutantRepository` - Pollutant lookups
- `CountryRepository` - Country lookups

## 🔧 Configuration

### Environment Variables

Create `.env` file in `.docker/` directory (optional, defaults provided):

```env
# Database
POSTGRES_PASSWORD=your_secure_password_here
POSTGRES_PORT=5432

# API
API_PORT=8000

# Grafana
GRAFANA_USER=admin
GRAFANA_PASSWORD=admin
GRAFANA_PORT=3000

# PgAdmin (optional)
PGADMIN_EMAIL=admin@discomap.com
PGADMIN_PASSWORD=admin
PGADMIN_PORT=5050
```

### API Configuration

Edit `src/config.py` for:
- Default countries and pollutants
- Data directories (if not using Docker volumes)
- Logging configuration
- Database connection settings

## ?? Security

- Non-root user in containers (UID 1000)
- Read-only config mounts
- Environment-based secrets
- Automated vulnerability scanning (Trivy)
- Network isolation

## ?? Production Deployment

### Cloud Providers

**AWS:**
```bash
# ECS/Fargate deployment
aws ecs create-cluster --cluster-name discomap
# Use docker-compose with ECS context
```

**Azure:**
```bash
# Container Instances
az container create --resource-group discomap-rg --file docker-compose.yml
```

**GCP:**
```bash
# Cloud Run
gcloud run deploy discomap --source .
```

### Kubernetes

```bash
# Convert docker-compose to k8s manifests
kompose convert -f docker-compose.yml

# Apply to cluster
kubectl apply -f .
```

See [DOCKER_GUIDE.md](docs/DOCKER_GUIDE.md) for detailed production deployment instructions.

## ?? Data Sources

All data from **European Environment Agency (EEA)**:
- **API Endpoint**: https://eeadmz1-downloads-api-appservice.azurewebsites.net
- **Direct Parquet URLs**: Bypass API date limitations
- **Pollutants**: PM10, PM2.5, O3, NO2, SO2, CO, Benzene, Heavy Metals, VOCs
- **Coverage**: 30+ European countries
- **Format**: Parquet files with hourly measurements
- **Temporal Resolution**: Hourly data from 1990 to present

### Known API Limitations

The EEA Parquet API endpoint has a known issue where `dateTimeStart` and `dateTimeEnd` parameters are **ignored**, always returning 2024-2025 data regardless of requested dates.

**Workaround**: Use the `/sync/get-urls` endpoint to retrieve direct Parquet file URLs, then sync with `from_urls` mode. This bypasses the API and downloads the correct historical data.

## ?? Contributing

```bash
# Clone and setup
git clone https://github.com/cost98/discomap.git
cd discomap

# Install dependencies
pip install -r .docker/requirements.txt

# Run tests
pytest

# Format code
black src/
isort src/
ruff check src/

# Create feature branch
git checkout -b feature/new-feature

# Commit changes
git commit -m "feat: add new feature"

# Push and create PR
git push origin feature/new-feature
```

### Development Stack

- **Python**: 3.11+
- **Database**: PostgreSQL 16 + TimescaleDB 2.14
- **ORM**: SQLAlchemy 2.0 (async)
- **Migrations**: Alembic 1.13+
- **API**: FastAPI 0.115+
- **Driver**: AsyncPG 0.29+
- **Testing**: pytest + pytest-asyncio
- **Formatting**: black, isort, ruff

## ?? License

This project uses data from the European Environment Agency (EEA). Please refer to [EEA_data_policy.md](EEA_data_policy.md) for data usage terms.

## ?? Support

- **Issues**: https://github.com/cost98/discomap/issues
- **Docs**: [docs/](docs/)

## ?? Roadmap

- [x] PostgreSQL + TimescaleDB integration
- [x] Docker containerization
- [x] Automated sync scheduler
- [x] PgAdmin and Grafana monitoring
- [x] REST API (FastAPI)
- [x] SQLAlchemy 2.0 ORM with async support
- [x] Alembic database migrations
- [x] Repository pattern architecture
- [ ] Complete db_writer.py refactoring to SQLAlchemy
- [ ] Async sync_scheduler.py
- [ ] Streamlit dashboard
- [ ] Prometheus metrics export
- [ ] Email/Slack alerts
- [ ] Data retention automation

---

Made with ?? for air quality monitoring


