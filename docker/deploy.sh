#!/bin/bash
# DiscoMap Production Deployment Script
# Usage: ./deploy.sh [version]
# Example: ./deploy.sh v1.0.0  (or omit for latest)

set -e

VERSION=${1:-latest}
COMPOSE_FILE="docker-compose.prod.yml"
ENV_FILE=".env.production"

echo "======================================"
echo "DiscoMap Production Deployment"
echo "======================================"
echo "Version: $VERSION"
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Error: Docker is not running"
    exit 1
fi

# Check if environment file exists
if [ ! -f "$ENV_FILE" ]; then
    echo "❌ Error: $ENV_FILE not found"
    echo "Create it from .env.production.example"
    exit 1
fi

# Check for default passwords
if grep -q "changeme" "$ENV_FILE"; then
    echo "⚠️  Warning: Default passwords detected in $ENV_FILE"
    read -p "Continue anyway? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Pull latest images
echo "📦 Pulling Docker images (version: $VERSION)..."
if [ "$VERSION" = "latest" ]; then
    docker-compose -f $COMPOSE_FILE pull
else
    # Update image tags in compose file temporarily
    sed -i.bak "s/:latest/:$VERSION/g" $COMPOSE_FILE
    docker-compose -f $COMPOSE_FILE pull
    mv $COMPOSE_FILE.bak $COMPOSE_FILE
fi

# Stop existing containers
echo "🛑 Stopping existing containers..."
docker-compose -f $COMPOSE_FILE down

# Start services
echo "🚀 Starting services..."
docker-compose -f $COMPOSE_FILE --env-file $ENV_FILE up -d

# Wait for services to be healthy
echo "⏳ Waiting for services to be healthy..."
sleep 10

# Check status
echo ""
echo "📊 Service Status:"
docker-compose -f $COMPOSE_FILE ps

# Show logs
echo ""
echo "📝 Recent logs:"
docker-compose -f $COMPOSE_FILE logs --tail=20

echo ""
echo "✅ Deployment complete!"
echo ""
echo "Services available at:"
echo "  - API: http://localhost:8000"
echo "  - Grafana: http://localhost:3000"
echo "  - pgAdmin: http://localhost:5050 (if tools profile enabled)"
echo ""
echo "To view logs: docker-compose -f $COMPOSE_FILE logs -f"
echo "To stop: docker-compose -f $COMPOSE_FILE down"
