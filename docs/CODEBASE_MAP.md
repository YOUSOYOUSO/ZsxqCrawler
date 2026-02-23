# Codebase Map

## Goal

This document provides a quick navigation map for humans and AI agents.

## Service Extraction Progress

- `api/services/global_crawl_service.py`: owns global crawl flow
- `api/services/global_analyze_service.py`: owns global performance analyze flow
- `api/services/global_file_task_service.py`: owns global file collect/download flows

## Top-Level Entry Files (compat shims)

- `main.py`: FastAPI app entry + compatibility glue
- `auto_scheduler.py`: scheduler orchestration
- `stock_analyzer.py`: compatibility shim for stock analyzer
- `global_analyzer.py`: compatibility shim for global analytics
- `ai_analyzer.py`: compatibility shim for AI analysis

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

## Compatibility Layer

The original root files are preserved as import shims to avoid breaking existing imports.

Example:

- `zsxq_database.py` re-exports from `modules.zsxq.zsxq_database`
- `stock_analyzer.py` re-exports from `modules.analyzers.stock_analyzer`
- `db_path_manager.py` re-exports from `modules.shared.db_path_manager`

This allows gradual migration without immediate large-scale refactoring.

## Migration Direction

- New internal code should prefer `modules/*` paths.
- Existing imports can be migrated gradually.
- Remove root shim files only after all references are migrated.
