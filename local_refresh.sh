#!/bin/bash
set -euo pipefail

REPO_DIR="$HOME/Documents/cursor/cc-factory-ai-dashboard"
LOG_FILE="$REPO_DIR/refresh.log"

exec > >(tee -a "$LOG_FILE") 2>&1
echo ""
echo "=== Dashboard refresh started at $(date) ==="

cd "$REPO_DIR"

export DATABRICKS_TOKEN
DATABRICKS_TOKEN=$(security find-generic-password -s "databricks-cc-dashboard" -a "jacqueline.ponce" -w 2>/dev/null || true)

if [ -z "$DATABRICKS_TOKEN" ]; then
  echo "ERROR: No token found in Keychain. Run:"
  echo "  security add-generic-password -s 'databricks-cc-dashboard' -a 'jacqueline.ponce' -w 'YOUR_PAT_HERE'"
  exit 1
fi

git pull --rebase --quiet 2>/dev/null || true

python3 refresh.py

git add index.html
if git diff --staged --quiet; then
  echo "No changes to dashboard"
else
  git commit -m "Auto-refresh dashboard $(date -u +%Y-%m-%d)"
  git push --quiet
  echo "Dashboard pushed to GitHub Pages"
fi

echo "=== Refresh completed at $(date) ==="
