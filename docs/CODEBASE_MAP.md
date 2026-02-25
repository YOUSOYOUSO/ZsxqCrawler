# Codebase Map

## Goal

This document provides a quick navigation map for humans and AI agents.

## Service Extraction Progress

- `api/services/group_filter_service.py`: shared group whitelist/blacklist filtering helper
- `api/services/global_crawl_service.py`: owns global crawl flow
- `api/services/global_analyze_service.py`: owns global performance analyze flow
- `api/services/global_file_task_service.py`: owns global file collect/download flows
- `api/services/task_facade.py`: task state/log/stop/stream facade
- `api/services/stock_scan_service.py`: group stock scan task orchestration
- `api/services/global_scan_filter_service.py`: scan-filter config + preview read/write
- `api/services/database_stats_service.py`: aggregated topic/file database statistics
- `api/services/columns_service.py`: columns read/fetch/stat/cleanup/comments orchestration
- `api/services/crawl_service.py`: crawl task orchestration + crawl settings persistence
- `api/services/file_service.py`: file collect/download/status/list/cleanup orchestration
- `api/services/topic_service.py`: topics/tags/group-topics read-write and cleanup orchestration
- `api/services/media_service.py`: image proxy/cache and local image/video serving
- `api/services/account_resolution_service.py`: account-group auto detection/cache and cookie resolution
- `api/services/runtime_settings_service.py`: runtime crawler/downloader settings read-write
- `api/services/account_service.py`: account CRUD + self-profile sync endpoints orchestration
- `api/services/group_service.py`: group info fallback and local group data cleanup
- `api/services/config_service.py`: app config read/write and configured-state resolution

## Top-Level Entry Files

- `app/main.py`: FastAPI app entry (lifespan/CORS/router registration/scheduler bridge only)
- `app/scheduler/auto_scheduler.py`: scheduler orchestration
- `app/runtime/image_cache_manager.py`: runtime image cache service

## Runtime Config Assets

- `config/stock_exclude.json`: stock exclusion rules (primary)
- `config/group_scan_filter.json`: global group filter configuration
- `config/stock_aliases.json`: stock alias dictionary
- `config/accounts.json`: legacy JSON account storage (deprecated, SQL-first)

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
- `modules/shared/paths.py`

## Migration Direction

- New internal code should prefer `modules/*` paths.
- Internal imports should directly use `modules/*` and `api/*` paths.
- Root compatibility shim files have been removed.
