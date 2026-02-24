# Codebase Map

## Goal

This document provides a quick navigation map for humans and AI agents.

## Service Extraction Progress

- `api/services/group_filter_service.py`: shared group whitelist/blacklist filtering helper
- `api/services/global_crawl_service.py`: owns global crawl flow
- `api/services/global_analyze_service.py`: owns global performance analyze flow
- `api/services/global_file_task_service.py`: owns global file collect/download flows

## Top-Level Entry Files

- `main.py`: FastAPI app entry + compatibility glue
- `auto_scheduler.py`: scheduler orchestration

## Runtime Config Assets

- `config/stock_exclude.json`: stock exclusion rules (primary)
- `stock_exclude.json`: legacy fallback path (read-only compatibility)
- `group_scan_filter.json`: global group filter configuration
- `stock_aliases.json`: stock alias dictionary

## Categorized Module Directories

### `modules/accounts/`

Account and auth-related internals:

- `modules/accounts/account_info_db.py`
- `modules/accounts/accounts_manager.py`
- `modules/accounts/accounts_sql_manager.py`
- `modules/accounts/migrate_accounts_to_sql.py`

### `modules/zsxq/`

ZSXQ crawling and storage internals:

- `modules/zsxq/zsxq_database.py`
- `modules/zsxq/zsxq_file_database.py`
- `modules/zsxq/zsxq_columns_database.py`
- `modules/zsxq/zsxq_file_downloader.py`
- `modules/zsxq/zsxq_interactive_crawler.py`

### `modules/analyzers/`

Analysis and strategy internals:

- `modules/analyzers/stock_analyzer.py`
- `modules/analyzers/global_analyzer.py`
- `modules/analyzers/ai_analyzer.py`
- `modules/analyzers/global_pipeline.py`
- `modules/analyzers/sector_heat.py`

### `modules/shared/`

Shared infrastructure and cross-domain helpers:

- `modules/shared/db_path_manager.py`
- `modules/shared/logger_config.py`
- `modules/shared/group_scan_filter.py`
- `modules/shared/stock_exclusion.py`

## Migration Direction

- New internal code should prefer `modules/*` paths.
- Internal imports should directly use `modules/*` and `api/*` paths.
- Root compatibility shim files have been removed.
