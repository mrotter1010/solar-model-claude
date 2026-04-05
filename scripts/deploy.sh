#!/usr/bin/env bash
set -e

# ---------------------------------------------------------------------------
# Deploy solar-model-claude to production VPS
# Run from your local machine: ./scripts/deploy.sh
# ---------------------------------------------------------------------------

VPS_HOST="root@87.99.136.159"
BRANCH="feature/m23-beta-deployment"
REPO_DIR="/root/solar-model-claude"
COMPOSE="docker compose -f docker-compose.yml -f docker-compose.prod.yml"

echo "==> Deploying branch '${BRANCH}' to ${VPS_HOST}..."

ssh "${VPS_HOST}" bash -s <<EOF
set -e

echo "==> Pulling latest code..."
cd ${REPO_DIR}
git pull origin ${BRANCH}

echo "==> Loading production environment..."
set -a
source .env.production
set +a

echo "==> Building and starting services..."
${COMPOSE} up --build -d

echo "==> Service status:"
${COMPOSE} ps

echo "==> Deploy complete."
EOF
