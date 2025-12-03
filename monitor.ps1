#!/usr/bin/env pwsh
# DiscoMap - Monitor Script
# Monitora lo stato del sync e del database

Write-Host "`n==================================================" -ForegroundColor Blue
Write-Host "📊 DISCOMAP - SYNC MONITOR" -ForegroundColor Blue
Write-Host "==================================================" -ForegroundColor Blue

Write-Host "`n🔍 CONTAINER STATUS:" -ForegroundColor Yellow
docker ps --filter "name=discomap" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

Write-Host "`n📊 DATABASE STATS:" -ForegroundColor Yellow
docker exec discomap-postgres psql -U discomap -d discomap -c "
SELECT 
    '📈 Total' as metric, 
    COUNT(*)::text as value 
FROM airquality.measurements
UNION ALL
SELECT 
    '🕒 Last Update', 
    MAX(time)::text 
FROM airquality.measurements
UNION ALL
SELECT 
    '🌍 Countries', 
    COUNT(DISTINCT sp.country_code)::text
FROM airquality.measurements m
JOIN airquality.sampling_points sp ON m.sampling_point_id = sp.sampling_point_id
UNION ALL
SELECT 
    '🧪 Pollutants', 
    COUNT(DISTINCT pollutant_code)::text
FROM airquality.measurements;
"

Write-Host "`n🌍 MEASUREMENTS BY COUNTRY:" -ForegroundColor Yellow
docker exec discomap-postgres psql -U discomap -d discomap -c "
SELECT 
    sp.country_code as country,
    COUNT(*) as measurements,
    ROUND(COUNT(*)::numeric * 100.0 / SUM(COUNT(*)) OVER (), 1)::text || '%' as percentage
FROM airquality.measurements m
JOIN airquality.sampling_points sp ON m.sampling_point_id = sp.sampling_point_id
GROUP BY sp.country_code
ORDER BY measurements DESC;
"

Write-Host "`n🧪 MEASUREMENTS BY POLLUTANT:" -ForegroundColor Yellow
docker exec discomap-postgres psql -U discomap -d discomap -c "
SELECT 
    p.pollutant_name as pollutant,
    COUNT(*) as measurements
FROM airquality.measurements m
JOIN airquality.pollutants p ON m.pollutant_code = p.pollutant_code
GROUP BY p.pollutant_name
ORDER BY measurements DESC;
"

Write-Host "`n📋 RECENT SYNC LOG (Last 20 lines):" -ForegroundColor Yellow
docker logs discomap-sync --tail=20

Write-Host "`n==================================================" -ForegroundColor Blue
Write-Host "💡 COMMANDS:" -ForegroundColor Cyan
Write-Host "  Live logs:     docker logs -f discomap-sync" -ForegroundColor White
Write-Host "  Force full:    docker exec discomap-sync python src/sync_scheduler.py --full" -ForegroundColor White
Write-Host "  Restart:       docker restart discomap-sync" -ForegroundColor White
Write-Host "==================================================" -ForegroundColor Blue
