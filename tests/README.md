# DiscoMap Tests

Test suite completa per database, ETL pipeline e servizi.

> **💡 I test sono anche la migliore documentazione!** Ogni test mostra esempi d'uso pratici.

## 🚀 Quick Start

```bash
# Tutti i test
pytest

# Test ETL Pipeline (esempi d'uso completi)
pytest tests/integration/test_etl_pipeline.py -v
pytest tests/unit/test_parquet_parser.py -v

# Con coverage
pytest --cov=src --cov-report=html

# Solo unit tests (veloce, no DB)
pytest tests/unit/ -v

# Solo integration tests (con DB)
pytest tests/integration/ -v
```

## 📁 Struttura

```
tests/
├── unit/
│   ├── test_config.py              # Configuration tests
│   ├── test_parquet_downloader.py  # Downloader tests
│   └── test_parquet_parser.py      # Parser tests (ESEMPI!)
├── integration/
│   ├── test_models.py              # SQLAlchemy model tests
│   ├── test_repositories.py        # Repository pattern tests
│   ├── test_etl_pipeline.py        # ETL completo (ESEMPI!)
│   └── test_timescaledb.py         # TimescaleDB tests
├── performance/
│   ├── test_parquet_downloader_*.py
│   └── ...
├── conftest.py                     # Pytest fixtures globali
└── README.md
```

## 📖 Test come Esempi d'Uso

### ParquetParser - Unit Tests

**File**: [tests/unit/test_parquet_parser.py](unit/test_parquet_parser.py)

```python
# Esempio: Parse completo di un file Parquet
def test_parse_all(sample_eea_dataframe, tmp_path):
    parser = ParquetParser()
    data = parser.parse_all(Path("file.parquet"))
    
    print(f"Stations: {len(data['stations'])}")
    print(f"Sampling Points: {len(data['sampling_points'])}")
    print(f"Measurements: {len(data['measurements'])}")

# Esempio: Gestione valori null
def test_null_values_handling():
    df_with_nulls = pd.DataFrame({...})
    parser = ParquetParser()
    stations = parser.parse_stations(df_with_nulls)
```

### ETL Pipeline - Integration Tests

**File**: [tests/integration/test_etl_pipeline.py](integration/test_etl_pipeline.py)

```python
# Esempio: ETL completo da file a database
async def test_etl_from_file(db_session, sample_parquet_file):
    pipeline = ETLPipeline()
    stats = await pipeline.run_from_file(Path("data.parquet"))
    
    assert stats["stations"] == 2
    assert stats["measurements"] == 3

# Esempio: Configurare batch size per performance
async def test_etl_batch_size(db_session, tmp_path):
    # Small batch per ambienti con poca RAM
    pipeline = ETLPipeline(batch_size=500)
    
    # Large batch per performance
    pipeline = ETLPipeline(batch_size=5000)
    
    stats = await pipeline.run_from_file(filepath)

# Esempio: Idempotenza (può essere eseguito più volte)
async def test_etl_idempotency(db_session, sample_parquet_file):
    pipeline = ETLPipeline()
    
    # Prima esecuzione
    stats1 = await pipeline.run_from_file(filepath)
    
    # Seconda esecuzione (aggiorna dati esistenti)
    stats2 = await pipeline.run_from_file(filepath)
```

### Repository Pattern - Integration Tests

**File**: [tests/integration/test_repositories.py](integration/test_repositories.py)

```python
# Esempio: Creare/aggiornare stazioni
async def test_station_create(db_session, sample_station_data):
    repo = StationRepository(db_session)
    station = await repo.create_or_update(sample_station_data)
    
    assert station.station_code == "TEST001"

# Esempio: Bulk insert measurements
async def test_measurement_bulk_insert(db_session):
    repo = MeasurementRepository(db_session)
    measurements = [{...}, {...}]
    count = await repo.bulk_insert(measurements)
```
