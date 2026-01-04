# Runbook

## Prerequisites

- Python 3.12+
- Network access to `bi.biaoguoworks.com` and `open.feishu.cn`
- Environment variables for BI and Feishu credentials

## Config

- `config/config.json` controls charts, outputs, and targets.
- Replace `config/targets/targets_a.csv` and `targets_b.csv` weekly (full refresh).

## Commands

```bash
python -m src --config config/config.json validate-config
python -m src --config config/config.json extract --date 2025-01-01
python -m src --config config/config.json load --date 2025-01-01
python -m src --config config/config.json transform --date 2025-01-01
python -m src --config config/config.json publish --date 2025-01-01
python -m src --config config/config.json run --date 2025-01-01
python -m src --config config/config.json backfill --start 2025-01-01 --end 2025-01-07
python -m src --config config/config.json compare --date 2025-01-01
```

## Scheduling (Linux)

```bash
sudo cp scripts/bi-pipeline.service /etc/systemd/system/
sudo cp scripts/bi-pipeline.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now bi-pipeline.timer
```

Logs are written to `logs/` and `data/reports/`.
