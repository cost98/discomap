# DiscoMap Tests

Test suite per il database layer (SQLAlchemy + AsyncPG).

## Esecuzione

```bash
# Tutti i test
pytest

# Test specifico
pytest tests/test_repositories.py

# Con coverage
pytest --cov=src --cov-report=html

# Solo test veloci (escludi integration)
pytest -m "not slow"

# Verbose
pytest -v
```

## Struttura

```
tests/
├── __init__.py
├── conftest.py              # Pytest fixtures
├── test_models.py           # SQLAlchemy model tests
├── test_repositories.py     # Repository pattern tests
├── test_config.py           # Configuration tests
└── README.md
```

## Fixtures Disponibili

- `db_session` - AsyncSession per test database
- `test_engine` - SQLAlchemy async engine
- `sample_station_data` - Dati station di esempio
- `sample_sampling_point_data` - Dati sampling point di esempio

## Note

- I test usano SQLite in-memory per velocità
- `conftest.py` setup/teardown automatico del database
- Async tests con `pytest-asyncio`
- Coverage report in `htmlcov/index.html`
