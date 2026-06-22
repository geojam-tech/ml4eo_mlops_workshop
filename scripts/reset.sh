#!/usr/bin/env bash
# Clean slate: nuke all runtime state and rebuild from git + code.
# data/seed/ (the committed, token-free AIS seed) is preserved.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> Stopping stack and removing volumes"
docker compose down -v

echo "==> Removing runtime artifacts (keeping data/seed)"
rm -f data/metrics.db data/mlflow.db
rm -rf data/cache data/mlartifacts mlruns

echo "==> Rebuilding and starting the stack"
docker compose up -d --build

cat <<'EOF'

Clean slate is up. Finish in the Prefect UI (http://localhost:4200):
  1. run "setup"      -> schema, water mask, AIS seed, registered CFAR models
  2. run "monitoring" -> populates predictions for the default model

Then Grafana is at http://localhost:3000 (admin / workshop).
EOF
