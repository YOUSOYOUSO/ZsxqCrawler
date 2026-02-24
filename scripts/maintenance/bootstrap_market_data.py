#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Bootstrap persistent AkShare market database."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from modules.analyzers.market_data_sync import MarketDataSyncService


def main() -> int:
    parser = argparse.ArgumentParser(description="回填本地持久化 AkShare 行情库")
    parser.add_argument("--no-resume", action="store_true", help="不从断点续跑，强制从头开始")
    parser.add_argument("--symbol-limit", type=int, default=0, help="仅处理前 N 只股票（调试用）")
    parser.add_argument("--batch-size", type=int, default=0, help="进度日志批次大小（覆盖配置）")
    args = parser.parse_args()

    svc = MarketDataSyncService()
    print("===> 步骤1/2: 同步股票代码表")
    symbol_res = svc.sync_symbols()
    print(symbol_res)
    if not symbol_res.get("success", False):
        return 1

    print("===> 步骤2/2: 全历史回填")
    res = svc.backfill_history_full(
        resume=not args.no_resume,
        batch_size=args.batch_size or None,
        symbol_limit=args.symbol_limit or None,
    )
    print(res)
    return 0 if res.get("success", False) else 2


if __name__ == "__main__":
    raise SystemExit(main())
