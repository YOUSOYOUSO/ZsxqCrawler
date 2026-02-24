#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def test_new_layout_import_chain_is_available():
    import modules.shared.db_path_manager as db_path_manager
    import modules.shared.group_scan_filter as group_scan_filter
    import modules.shared.stock_exclusion as stock_exclusion
    import modules.analyzers.global_pipeline as global_pipeline

    assert Path("app/main.py").exists()
    assert Path("app/scheduler/auto_scheduler.py").exists()
    assert Path("app/runtime/image_cache_manager.py").exists()
    assert hasattr(db_path_manager, "get_db_path_manager")
    assert hasattr(global_pipeline, "run_serial_incremental_pipeline")
    assert hasattr(group_scan_filter, "filter_groups")
    assert hasattr(stock_exclusion, "is_excluded_stock")
