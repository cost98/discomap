# DiscoMap Docker Configuration

This directory contains Docker configuration for DiscoMap production deployment.

## Services

### PostgreSQL + TimescaleDB
- **Image**: Custom build from `Dockerfile.postgres`
- **Purpose**: Time-series database with Alembic migrations
- **Port**: 5432 (configurable via `POSTGRES_PORT`)
- **Schema Management**: Alembic (single source of truth from SQLAlchemy models)

### API Server
- **Image**: Custom build from `Dockerfile.api`
- **Purpose**: REST API for data sync and operations
- **Port**: 8000 (configurable via `API_PORT`)

### PgAdmin (Optional)
- **Profile**: `tools`
- **Purpose**: Database management UI
- **Port**: 5050 (configurable via `PGADMIN_PORT`)

### Grafana
- **Purpose**: Data visualization and dashboards
- **Port**: 3000 (configurable via `GRAFANA_PORT`)

## Quick Start

1. **Copy environment file**:
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

2. **Start core services** (PostgreSQL + API):
   ```bash
   docker-compose up -d
   ```

3. **Start with management tools**:
   ```bash
   docker-compose --profile tools up -d
   ```

4. **View logs**:
   ```bash
   docker-compose logs -f
   ```

5. **Stop services**:
   ```bash
   docker-compose down
   ```

## Database Migrations (Alembic)

### Schema Management
The database schema is managed by **Alembic**, which generates migrations from SQLAlchemy models.

**Single source of truth**: `src/database/models/`

### Automatic Migrations
Migrations run automatically when the PostgreSQL container starts:
1. PostgreSQL initializes
2. TimescaleDB extension is enabled
3. Alembic runs `upgrade head`
4. Database is ready!

### Manual Migration Operations

**Create a new migration** (after changing models):
```bash
cd .docker
export DB_HOST=localhost
export DB_PASSWORD=your_password
alembic revision --autogenerate -m "Description of changes"
```

**Apply migrations**:
```bash
cd .docker
alembic upgrade head
```

**Rollback last migration**:
```bash
cd .docker
alembic downgrade -1
```

**View migration history**:
```bash
cd .docker
alembic history
```

### Migration Files
- **Location**: `.docker/alembic/versions/`
- **Initial migration**: `001_initial.py` (creates all tables from SQLAlchemy models)
- **Format**: `YYYYMMDD_HHMM_<revision>_<slug>.py`

## Directory Structure

```
.docker/
├── alembic/                      # Alembic migrations
│   ├── versions/                 # Migration scripts
│   │   └── 001_initial.py       # Initial schema
│   ├── env.py                    # Alembic environment
│   └── script.py.mako            # Migration template
├── alembic.ini                   # Alembic configuration
├── postgres/                     # PostgreSQL configuration
│   ├── docker-entrypoint-migrations.sh  # Migration runner
│   ├── postgresql.conf           # PostgreSQL tuning (if needed)
│   └── backups/                  # Database backups (volume mount)
├── pgadmin/                      # PgAdmin configuration
│   └── servers.json              # Pre-configured servers
├── grafana/                      # Grafana configuration
│   ├── datasources/              # Data sources
│   └── dashboards/               # Dashboards
├── docker-compose.yml            # Main compose file
├── docker-compose.prod.yml       # Production overrides
├── Dockerfile.postgres           # PostgreSQL + TimescaleDB image
├── Dockerfile.api                # API server image
├── .env.example                  # Environment variables template
└── README.md                     # This file
```

## Environment Variables

See `.env.example` for all available configuration options.

### Key Variables
- `POSTGRES_PASSWORD`: Database password (change in production!)
- `DB_HOST`: Database host (use `postgres` for Docker network)
- `API_PORT`: API server port
- `EEA_EMAIL`: Email for EEA API access

## Data Persistence

### Volumes
- `postgres-data`: Database files
- `app-data`: Application data
- `app-logs`: Application logs
- `pgadmin-data`: PgAdmin settings
- `grafana-data`: Grafana dashboards

### Backups
PostgreSQL backups are mounted at `.docker/postgres/backups/`

**Create backup**:
```bash
docker exec discomap-postgres pg_dump -U discomap discomap > backups/backup_$(date +%Y%m%d).sql
```

**Restore backup**:
```bash
docker exec -i discomap-postgres psql -U discomap discomap < backups/backup_20250101.sql
```

## Development vs Production

### Development (Current Setup)
- Uses `.env` file
- Hot reload enabled
- Debug logging
- Accessible on localhost

### Production (docker-compose.prod.yml)
- Uses environment variables from hosting provider
- Production-optimized settings
- Restricted access
- Health checks enabled

**Deploy to production**:
```bash
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

## Troubleshooting

### View container logs
```bash
docker-compose logs postgres
docker-compose logs api
```

### Access PostgreSQL shell
```bash
docker exec -it discomap-postgres psql -U discomap -d discomap
```

### Check Alembic migration status
```bash
docker exec -it discomap-postgres bash
cd /app
python3 -m alembic current
python3 -m alembic history
```

### Reset database (⚠️ destroys all data!)
```bash
docker-compose down -v
docker-compose up -d
```

## Notes

### Old SQL Scripts (Deprecated)
The following SQL scripts are **deprecated** and will be removed:
- `postgres/init-db.sql`
- `postgres/create-tables.sql`
- `postgres/create-hypertables.sql`
- `postgres/04-sync-tracking.sql`

**Use Alembic migrations instead** - they are generated from SQLAlchemy models and provide version control.

### Why Alembic?
1. **Single source of truth**: Schema defined in Python models
2. **Version control**: Track all schema changes
3. **Rollback support**: Undo migrations if needed
4. **Auto-generation**: Detect model changes automatically
5. **Consistency**: Same schema in dev, test, and production

## Support

For issues or questions:
- Check logs: `docker-compose logs -f`
- Verify health: `docker-compose ps`
- Inspect network: `docker network inspect discomap-network`
