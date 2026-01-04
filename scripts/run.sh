#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# Activate virtualenv if present
if [[ -f ".venv/bin/activate" ]]; then
  source ".venv/bin/activate"
fi

python -m src --config config/config.json run