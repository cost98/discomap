# SQLAlchemy Migration Guide

## Overview

This guide explains the migration from the old `db_writer.py` (psycopg2) to the new SQLAlchemy 2.0 + AsyncPG architecture.

## Architecture Changes

### Before (Old)
```
db_writer.py
├── psycopg2 (synchronous driver)
├── Manual SimpleConnectionPool
├── Raw SQL queries
└── No ORM, no migrations
```

### After (New)
```
src/database/
├── models.py           # SQLAlchemy declarative models
├── engine.py           # Async engine & session factory
├── repositories.py     # Repository pattern for CRUD
└── __init__.py         # Package exports

alembic/               # Database migrations
├── versions/          # Migration scripts
└── env.py            # Alembic configuration
```

## Key Improvements

1. **Async/Await**: Full async support with AsyncPG driver (faster than psycopg2)
2. **Type Safety**: SQLAlchemy 2.0 with type hints (Mapped[T])
3. **ORM**: Automatic query generation, relationship loading
4. **Migrations**: Alembic for schema versioning (no more manual SQL scripts)
5. **Repository Pattern**: Clean separation of concerns
6. **Connection Pooling**: Built-in SQLAlchemy pool management

## Migration Steps

### Step 1: Update Dependencies ✅
Already done in `.docker/requirements.txt`:
- `sqlalchemy>=2.0.0`
- `alembic>=1.13.0`
- `asyncpg>=0.29.0`

### Step 2: Create Database Models ✅
SQLAlchemy models created in `src/database/models.py`:
- `Station` - Physical monitoring stations
- `SamplingPoint` - Sensors/instruments
- `Measurement` - Time-series data (TimescaleDB hypertable)
- `Country`, `Pollutant`, etc. - Dimension tables

### Step 3: Setup Engine & Sessions ✅
Created `src/database/engine.py` with:
```python
from src.database import get_db_session

async with get_db_session() as session:
    # Your database operations
    await session.commit()
```

### Step 4: Create Repositories ✅
Repository pattern in `src/database/repositories.py`:
```python
from src.database import StationRepository, get_db_session

async with get_db_session() as session:
    station_repo = StationRepository(session)
    station = await station_repo.get_by_code("IT0508A")
```

### Step 5: Initialize Alembic ✅
```bash
python -m alembic init alembic
```

Configured in `alembic/env.py` to:
- Auto-import models from `src.database.models`
- Use environment variables for DB connection
- Support `airquality` schema

### Step 6: Generate Initial Migration (TODO)
```bash
# Generate migration from current SQL schema
python -m alembic revision --autogenerate -m "initial schema"

# Review the migration in alembic/versions/

# Apply migration
python -m alembic upgrade head
```

### Step 7: Refactor db_writer.py (TODO)
Gradual migration strategy:

**Option A: Wrapper Pattern (Low Risk)**
```python
# db_writer.py - Keep existing interface
class DatabaseWriter:
    async def write_stations(self, stations: List[dict]):
        async with get_db_session() as session:
            repo = StationRepository(session)
            return await repo.bulk_create_or_update(stations)
```

**Option B: Full Replacement (High Impact)**
- Replace all `db_writer.py` calls in codebase
- Update `sync_scheduler.py` to use repositories
- Migrate all SQL queries to SQLAlchemy

## Code Comparison

### Old: db_writer.py (psycopg2)
```python
def write_station(station_data):
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO airquality.stations (station_code, ...)
                VALUES (%s, ...)
                ON CONFLICT (station_code) DO UPDATE SET ...
            """, (station_data['station_code'], ...))
        conn.commit()
    finally:
        pool.putconn(conn)
```

### New: SQLAlchemy Repository
```python
async def write_station(station_data):
    async with get_db_session() as session:
        repo = StationRepository(session)
        return await repo.create_or_update(station_data)
```

## Performance Considerations

### Bulk Inserts
For large measurement batches, use `bulk_insert_mappings`:

```python
from sqlalchemy import insert

async with get_db_session() as session:
    await session.execute(
        insert(Measurement),
        measurements  # List of dicts
    )
    await session.commit()
```

### TimescaleDB Compatibility
- SQLAlchemy models work with TimescaleDB hypertables
- Use composite primary key `(time, sampling_point_id)`
- Indexes defined in model match SQL schema

### Connection Pooling
```python
engine = create_async_engine(
    db_url,
    pool_size=5,          # Min connections
    max_overflow=10,      # Extra connections
    pool_pre_ping=True,   # Test before use
)
```

## Testing

### Unit Tests (TODO)
```python
import pytest
from src.database import get_db_session, StationRepository

@pytest.mark.asyncio
async def test_station_create():
    async with get_db_session() as session:
        repo = StationRepository(session)
        station = await repo.create_or_update({
            "station_code": "TEST001",
            "country_code": "IT",
        })
        assert station.station_code == "TEST001"
```

### Integration Tests
Run `test_sqlalchemy.py` to verify:
```bash
python test_sqlalchemy.py
```

## Migration Checklist

- [x] Install SQLAlchemy, Alembic, AsyncPG
- [x] Create database models
- [x] Setup async engine & sessions
- [x] Create repository pattern
- [x] Initialize Alembic
- [ ] Generate initial migration
- [ ] Test migrations on dev database
- [ ] Refactor db_writer.py methods one by one
- [ ] Update sync_scheduler.py
- [ ] Update api_server.py endpoints
- [ ] Add comprehensive tests
- [ ] Deploy to production

## Rollback Plan

If issues arise:
1. Keep old `db_writer.py` intact during migration
2. Use feature flags to switch between old/new
3. SQL initialization scripts still work (fallback)
4. Alembic migrations are reversible: `alembic downgrade -1`

## Next Steps

1. **Generate first migration**: `alembic revision --autogenerate -m "initial"`
2. **Test on dev database**: Ensure migration creates correct schema
3. **Create db_writer wrapper**: Maintain backward compatibility
4. **Gradual refactor**: Replace one method at a time
5. **Add tests**: Cover all repository operations
6. **Documentation**: Update API docs, deployment guide

## Resources

- [SQLAlchemy 2.0 Docs](https://docs.sqlalchemy.org/en/20/)
- [Alembic Tutorial](https://alembic.sqlalchemy.org/en/latest/tutorial.html)
- [AsyncPG Performance](https://github.com/MagicStack/asyncpg)
- [FastAPI + SQLAlchemy](https://fastapi.tiangolo.com/tutorial/sql-databases/)
