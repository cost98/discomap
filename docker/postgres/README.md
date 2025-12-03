# DiscoMap PostgreSQL Initialization Scripts

Scripts eseguiti **in ordine alfabetico** al primo avvio del container PostgreSQL.

## Ordine di esecuzione

1. **`init-db.sql`** - Inizializzazione database
   - Abilita estensioni (TimescaleDB, pg_stat_statements)
   - Crea schema `airquality`
   - Crea ruoli e permessi

2. **`create-tables.sql`** - Creazione tabelle
   - **Dimension tables**: countries, pollutants, validity_flags, verification_status
   - **Stations**: Stazioni fisiche (location unica)
   - **Sampling points**: Sensori/strumenti alle stazioni (1 stazione → N sensori)
   - **Measurements**: Time-series dati (hypertable TimescaleDB)

3. **`create-hypertables.sql`** - Configurazione TimescaleDB
   - Converte `measurements` in hypertable (partitioning per tempo)
   - Crea continuous aggregates (hourly_stats, daily_stats)
   - Configura compression policies
   - Crea indici ottimizzati

4. **`04-sync-tracking.sql`** - Tracking operazioni sync
   - Tabella `sync_operations` per monitoraggio download
   - Views per operazioni correnti

## Schema Database

### Gerarchia Stazioni

```
stations (stazione fisica)
  ├─ station_code: IT0508A (identificativo unico)
  ├─ station_name: "Milano Corso Buenos Aires"
  ├─ location: lat/lon/altitude
  └─ metadata: tipo, area, comune, regione
      │
      └─► sampling_points (sensori/strumenti)
            ├─ sampling_point_id: IT/SPO.IT0508A_8_chemi_1990-09-27_00:00:00
            ├─ instrument_type: "8_chemi" (NO2)
            ├─ pollutant_code: 8 (NO2)
            └─ start_date/end_date
                │
                └─► measurements (misurazioni)
                      ├─ time: timestamp
                      ├─ value: concentrazione
                      ├─ validity: 1-4, -1, -99
                      └─ verification: 1-3
```

### Esempio Pratico

**Stazione fisica:** IT0508A (Milano Corso Buenos Aires)
- Coordinate: 45.4808°N, 9.2040°E
- Tipo: traffic, urban

**Sampling points alla stazione IT0508A:**
1. `IT/SPO.IT0508A_8_chemi_1990-09-27` → NO2 (chemical analyzer, dal 1990)
2. `IT/SPO.IT0508A_5_nephelometry_beta_2020-01-01` → PM10 (nephelometer, dal 2020)
3. `IT/SPO.IT0508A_7_UV-P_1990-12-01` → O3 (UV photometry, dal 1990)

Ogni sampling point ha le sue misurazioni nella tabella `measurements`.

## Reset Completo Database

```bash
# Stop containers
docker-compose down

# Remove volumes (ATTENZIONE: cancella tutti i dati!)
docker volume rm docker_postgres-data

# Restart (reinizializza da zero)
docker-compose up -d postgres
```

## Verifica Schema

```sql
-- Conteggio tabelle
SELECT 
    schemaname, 
    tablename, 
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables 
WHERE schemaname = 'airquality'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Stazioni e punti di campionamento
SELECT 
    s.station_code,
    s.station_name,
    COUNT(sp.sampling_point_id) as num_sensors,
    ARRAY_AGG(DISTINCT p.pollutant_name) as pollutants
FROM airquality.stations s
LEFT JOIN airquality.sampling_points sp ON s.station_code = sp.station_code
LEFT JOIN airquality.pollutants p ON sp.pollutant_code = p.pollutant_code
GROUP BY s.station_code, s.station_name
ORDER BY num_sensors DESC
LIMIT 10;
```

## Note

- **Validity codes**: -99 (maintenance), -1 (invalid), 1 (valid), 2 (below detection), 3 (above detection), 4 (ozone CCQM)
- **Verification codes**: 1 (verified), 2 (preliminary verified), 3 (not verified)
- **Hypertable partitioning**: Measurements partizionati per settimana
- **Compression**: Dati > 7 giorni compressi automaticamente
- **Retention**: Continuous aggregates calcolati automaticamente
