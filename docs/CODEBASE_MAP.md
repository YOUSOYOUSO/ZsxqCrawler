# Codebase Map

## Goal

This document provides a quick navigation map for humans and AI agents.

## Top-Level Entry Files (keep stable)

- `main.py`: FastAPI app entry + compatibility glue
- `auto_scheduler.py`: scheduler orchestration
- `stock_analyzer.py`: stock analysis engine (current canonical module)
- `global_analyzer.py`: cross-group analytics
- `ai_analyzer.py`: AI summary and reasoning workflows

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

## Compatibility Layer

The original root files are preserved as import shims to avoid breaking existing imports.

Example:

- `zsxq_database.py` re-exports from `modules.zsxq.zsxq_database`

This allows gradual migration without immediate large-scale refactoring.

## Migration Direction

- New internal code should prefer `modules/*` paths.
- Existing imports can be migrated gradually.
- Remove root shim files only after all references are migrated.
