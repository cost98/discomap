# DiscoMap - Production Deployment with GHCR

This guide explains how to deploy DiscoMap in production using pre-built images from GitHub Container Registry.

## Prerequisites

- Docker and Docker Compose installed
- Access to a Linux server (Ubuntu/Debian recommended)
- Domain name configured (optional, for SSL)

## Quick Start

### 1. Pull the images

```bash
# Login to GitHub Container Registry (if images are private)
echo $GITHUB_TOKEN | docker login ghcr.io -u USERNAME --password-stdin

# Pull latest images
docker pull ghcr.io/cost98/discomap-api:latest
docker pull ghcr.io/cost98/discomap-postgres:latest
docker pull grafana/grafana:latest
```

### 2. Setup environment

```bash
# Clone repo (or just download docker-compose files)
git clone https://github.com/cost98/discomap.git
cd discomap/docker

# Create production environment file
cp .env.example .env.production
nano .env.production
```

### 3. Configure `.env.production`

```env
# Database
POSTGRES_PASSWORD=<your-strong-password-here>
POSTGRES_PORT=5432

# API
API_PORT=8000

# Grafana
GRAFANA_ADMIN_PASSWORD=<your-strong-password-here>
GRAFANA_PORT=3000

# Optional: pgAdmin
PGADMIN_EMAIL=admin@yourdomain.com
PGADMIN_PASSWORD=<your-strong-password-here>
```

### 4. Deploy

```bash
# Start all services
docker-compose --env-file .env.production up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f
```

## Image Versioning

Images are automatically built and tagged:

- `latest` - Latest commit on main branch
- `v1.0.0` - Specific version tags
- `main-abc123` - Git commit SHA
- `pr-123` - Pull request builds

### Deploy specific version

```bash
# In docker-compose.yml, specify version:
services:
  api:
    image: ghcr.io/cost98/discomap-api:v1.0.0
  postgres:
    image: ghcr.io/cost98/discomap-postgres:v1.0.0
```

## Production Checklist

- [ ] Changed all default passwords
- [ ] Configured firewall rules
- [ ] Setup SSL/TLS with Let's Encrypt
- [ ] Configured backup for PostgreSQL data
- [ ] Setup monitoring and alerting
- [ ] Configured log rotation
- [ ] Tested disaster recovery procedure

## Backup & Restore

### Backup database

```bash
docker exec discomap-postgres pg_dump -U discomap -d discomap \
  | gzip > backup_$(date +%Y%m%d_%H%M%S).sql.gz
```

### Restore database

```bash
gunzip -c backup_20251204_120000.sql.gz | \
  docker exec -i discomap-postgres psql -U discomap -d discomap
```

## Updates

```bash
# Pull latest images
docker-compose pull

# Restart services
docker-compose up -d

# Check everything is running
docker-compose ps
```

## Troubleshooting

### Check container logs

```bash
docker-compose logs api
docker-compose logs postgres
docker-compose logs grafana
```

### Restart specific service

```bash
docker-compose restart api
```

### Reset database (WARNING: deletes all data!)

```bash
docker-compose down -v
docker-compose up -d
```

## Support

- GitHub Issues: https://github.com/cost98/discomap/issues
- Documentation: https://github.com/cost98/discomap/wiki
