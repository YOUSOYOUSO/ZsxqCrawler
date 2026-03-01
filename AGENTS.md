# AGENTS Rules

## Root Directory Policy

- Do not add business Python files to the repository root.
- Place runtime entry and orchestration code under `app/`.
- Place domain logic under `modules/`.
- Place one-off scripts under `scripts/maintenance/`.
- Place tests under `tests/`.

## Startup

- Backend startup command: `uv run app/main.py`.

## Config Paths

- Main config: `config/app.toml`
- Group filter config: `config/group_scan_filter.json`
- Stock alias config: `config/stock_aliases.json`

## Interaction Rules

- In every response, address the user as `BOSS`.
- In every response, use Chinese.
