# DiscoMap - Setup Completo Database Pulito

## 🎯 Obiettivo

Separare chiaramente **stazioni fisiche** (locations) da **punti di campionamento** (sensori/strumenti).

## 📊 Nuova Struttura

```
┌─────────────────────────────────────────────────────┐
│ STATION (Stazione Fisica)                           │
│ ├─ station_code: IT0508A                            │
│ ├─ station_name: "Milano Corso Buenos Aires"        │
│ ├─ location: (45.4808°N, 9.2040°E, 122m)           │
│ └─ type: traffic, urban                             │
└──────────────────┬──────────────────────────────────┘
                   │
       ┌───────────┴──────────────┬─────────────────┐
       │                          │                 │
       ▼                          ▼                 ▼
┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐
│ SAMPLING POINT 1 │   │ SAMPLING POINT 2 │   │ SAMPLING POINT 3 │
│ NO2 (8_chemi)    │   │ PM10 (5_BETA)    │   │ O3 (7_UV-P)      │
│ dal 1990         │   │ dal 2020         │   │ dal 1990         │
└────────┬─────────┘   └────────┬─────────┘   └────────┬─────────┘
         │                      │                      │
         └──────────────┬───────┴──────────────────────┘
                        │
                        ▼
              ┌─────────────────────┐
              │   MEASUREMENTS      │
              │   (time-series)     │
              │   ├─ timestamp      │
              │   ├─ value          │
              │   ├─ validity       │
              │   └─ verification   │
              └─────────────────────┘
```

## 📁 File Modificati

### SQL Scripts (ordine esecuzione)
1. ✅ `docker/postgres/init-db.sql` - Inizializzazione base
2. ✅ `docker/postgres/create-tables.sql` - **MODIFICATO**
   - Aggiunta tabella `stations` (stazioni fisiche)
   - Modificata tabella `sampling_points` (sensori)
   - Relationship: stations 1→N sampling_points
3. ✅ `docker/postgres/create-hypertables.sql` - Hypertables
4. ✅ `docker/postgres/04-sync-tracking.sql` - Sync tracking

### Python Code
- ✅ `src/db_writer.py` - **MODIFICATO**
  - Funzione `upsert_sampling_points()` ora:
    - Estrae station_code da sampling_point_id
    - Estrae instrument_type (e.g., 8_chemi, 5_BETA)
    - Crea automaticamente record in `stations`
    - Popola `sampling_points` con riferimenti corretti

### Utilities
- ✅ `docker/postgres/README.md` - Documentazione completa
- ✅ `docker/reset-database.ps1` - Script reset automatico

## 🚀 Reset Database Completo

### Opzione 1: Script Automatico (Consigliato)
```powershell
cd docker
.\reset-database.ps1
```

### Opzione 2: Manuale
```powershell
cd docker

# Stop containers
docker-compose down

# Remove volumes
docker volume rm docker_postgres-data
docker volume rm docker_pgadmin-data

# Restart
docker-compose up -d postgres

# Wait and verify
Start-Sleep -Seconds 15
docker exec discomap-postgres psql -U discomap -d discomap -c "\dt airquality.*"
```

## 📋 Verifica Schema

```sql
-- Elenco tabelle
SELECT schemaname, tablename 
FROM pg_tables 
WHERE schemaname = 'airquality' 
ORDER BY tablename;

-- Dovrebbe mostrare:
-- airquality | countries
-- airquality | measurements
-- airquality | pollutants
-- airquality | sampling_points
-- airquality | stations
-- airquality | sync_operations
-- airquality | validity_flags
-- airquality | verification_status
```

## 🔄 Primo Sync con Nuova Struttura

```bash
# Avvia tutti i servizi
docker-compose up -d

# Sync incrementale (ultimi 7 giorni, solo PM10/PM2.5)
curl.exe -X POST http://localhost:8000/sync/start \
  -H "Content-Type: application/json" \
  -d '{
    "sync_type": "incremental",
    "countries": ["IT"],
    "pollutants": ["PM10", "PM2.5"],
    "days": 7,
    "max_workers": 8
  }'

# Monitora progresso
curl.exe http://localhost:8000/sync/status/<sync_id>
```

## 🔍 Query Esempio

### Stazioni con conteggio sensori
```sql
SELECT 
    s.station_code,
    s.station_name,
    s.country_code,
    COUNT(sp.sampling_point_id) as num_sensors,
    ARRAY_AGG(DISTINCT p.pollutant_name) as pollutants
FROM airquality.stations s
LEFT JOIN airquality.sampling_points sp ON s.station_code = sp.station_code
LEFT JOIN airquality.pollutants p ON sp.pollutant_code = p.pollutant_code
GROUP BY s.station_code, s.station_name, s.country_code
ORDER BY num_sensors DESC
LIMIT 10;
```

### Dettaglio sensori per stazione
```sql
SELECT 
    sp.sampling_point_id,
    sp.station_code,
    s.station_name,
    sp.instrument_type,
    p.pollutant_name,
    sp.start_date,
    COUNT(m.time) as num_measurements
FROM airquality.sampling_points sp
JOIN airquality.stations s ON sp.station_code = s.station_code
LEFT JOIN airquality.pollutants p ON sp.pollutant_code = p.pollutant_code
LEFT JOIN airquality.measurements m ON sp.sampling_point_id = m.sampling_point_id
WHERE s.station_code = 'IT0508A'
GROUP BY sp.sampling_point_id, sp.station_code, s.station_name, 
         sp.instrument_type, p.pollutant_name, sp.start_date;
```

## ⚠️ Note Importanti

1. **Backup**: Se hai dati importanti, fai backup prima del reset
2. **Volume**: Reset cancella TUTTO (dati + schema)
3. **Tempo**: Primo sync può richiedere tempo (dipende dal periodo)
4. **Metadata Stazioni**: Dopo sync, importa metadati stazioni via CSV API

## 📊 Vantaggi Nuova Struttura

✅ **Separazione logica**: Stazione ≠ Sensore  
✅ **Normalizzazione**: No dati duplicati per location  
✅ **Query efficienti**: Join ottimizzati  
✅ **Metadata gestibile**: Info stazione separata da info sensore  
✅ **Scalabilità**: Aggiungi sensori senza modificare stazione  

## 🎯 Prossimi Passi

1. ✅ Reset database
2. ⏳ Avvia sync dati
3. ⏳ Importa metadata stazioni (CSV con lat/lon/nomi)
4. ⏳ Verifica dashboard Grafana
5. ⏳ Configura sync automatici
