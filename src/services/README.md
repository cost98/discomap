# ETL Pipeline - DiscoMap

Pipeline per estrazione, trasformazione e caricamento dati EEA Air Quality.

## Componenti

1. **[ParquetDownloader](parquet_downloader.py)** - Download file Parquet da Azure
2. **[ParquetParser](parquet_parser.py)** - Parse file EEA
3. **[ETLPipeline](etl_pipeline.py)** - Orchestrazione completa

## Esempi d'Uso

**Tutti gli esempi sono nei test:**
- [test_parquet_parser.py](../../tests/unit/test_parquet_parser.py)
- [test_etl_pipeline.py](../../tests/integration/test_etl_pipeline.py)

```bash
pytest tests/unit/test_parquet_parser.py -v
pytest tests/integration/test_etl_pipeline.py -v
```

## API Reference

### ETLPipeline
```python
pipeline = ETLPipeline(batch_size=1000)
stats = await pipeline.run_from_url("https://...")
stats = await pipeline.run_from_file(Path("data.parquet"))
```

### ParquetParser
```python
parser = ParquetParser()
data = parser.parse_all(Path("file.parquet"))
# Returns: {"stations": [...], "sampling_points": [...], "measurements": [...]}
```

## Performance

| Operazione | Tempo |
|------------|-------|
| Download 100MB | ~10s |
| Parse 1M rows | ~5s |
| Insert 1M measurements | ~30s |
