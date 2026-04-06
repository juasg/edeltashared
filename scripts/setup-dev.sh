#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

echo "=== eDeltaShared Dev Setup ==="

# Copy .env if it doesn't exist
if [ ! -f .env ]; then
    cp .env.example .env
    echo "[1/4] Created .env from .env.example"
else
    echo "[1/4] .env already exists, skipping"
fi

# Start infrastructure services
echo "[2/4] Starting infrastructure (Postgres, Kafka, Redis)..."
docker compose up -d postgres redis zookeeper kafka kafka-ui pgadmin

# Wait for services to be healthy
echo "[3/4] Waiting for services to be healthy..."
echo -n "  Postgres..."
until docker compose exec -T postgres pg_isready -U edeltashared > /dev/null 2>&1; do
    sleep 1
done
echo " ready"

echo -n "  Kafka..."
until docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do
    sleep 2
done
echo " ready"

echo -n "  Redis..."
until docker compose exec -T redis redis-cli ping > /dev/null 2>&1; do
    sleep 1
done
echo " ready"

# Install Python dependencies
echo "[4/4] Installing Python dependencies..."
if [ -d "backend" ] && [ -f "backend/requirements.txt" ]; then
    cd backend
    python3 -m venv .venv 2>/dev/null || true
    source .venv/bin/activate
    pip install -r requirements.txt --quiet
    cd "$PROJECT_ROOT"
    echo "  Backend dependencies installed"
fi

if [ -d "cdc-agent" ] && [ -f "cdc-agent/requirements.txt" ]; then
    cd cdc-agent
    python3 -m venv .venv 2>/dev/null || true
    source .venv/bin/activate
    pip install -r requirements.txt --quiet
    cd "$PROJECT_ROOT"
    echo "  CDC Agent dependencies installed"
fi

echo ""
echo "=== Dev environment ready ==="
echo "  Postgres:  localhost:5432"
echo "  Kafka:     localhost:9092"
echo "  Redis:     localhost:6379"
echo "  Kafka UI:  http://localhost:8080"
echo "  pgAdmin:   http://localhost:5050"
echo ""
echo "Next steps:"
echo "  cd backend && source .venv/bin/activate && uvicorn main:app --reload"
