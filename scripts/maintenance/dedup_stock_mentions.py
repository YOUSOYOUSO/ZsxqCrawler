#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
清洗 stock_mentions 重复提及数据，并清理 mention_performance 孤儿记录。

默认 dry-run，仅展示将要清理的数据量；
传入 --apply 后执行真实写入。

去重键: topic_id + UPPER(TRIM(stock_code))
保留策略:
1) mention_time 更大者优先
2) mention_time 相同则 id 更大者优先
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from modules.shared.db_path_manager import get_db_path_manager  # noqa: E402


@dataclass
class CleanStats:
    db_path: str
    total_before: int = 0
    distinct_pairs_before: int = 0
    duplicate_rows_before: int = 0
    duplicate_groups_before: int = 0
    normalized_codes_updated: int = 0
    removed_duplicate_rows: int = 0
    orphan_perf_before: int = 0
    removed_orphan_perf: int = 0
    total_after: int = 0
    distinct_pairs_after: int = 0
    duplicate_rows_after: int = 0


def _count_distinct_pairs(cursor: sqlite3.Cursor, topic_id: Optional[str]) -> int:
    where = ""
    params: List[object] = []
    if topic_id:
        where = "WHERE topic_id = ?"
        params.append(topic_id)
    cursor.execute(
        f"""
        SELECT COUNT(DISTINCT CAST(topic_id AS TEXT) || '#' || UPPER(TRIM(stock_code)))
        FROM stock_mentions
        {where}
        """,
        params,
    )
    return int((cursor.fetchone() or [0])[0] or 0)


def _count_duplicate_rows_and_groups(cursor: sqlite3.Cursor, topic_id: Optional[str]) -> tuple[int, int]:
    where = ""
    params: List[object] = []
    if topic_id:
        where = "WHERE topic_id = ?"
        params.append(topic_id)
    cursor.execute(
        f"""
        SELECT
          COALESCE(SUM(cnt - 1), 0) AS duplicate_rows,
          COUNT(*) AS duplicate_groups
        FROM (
          SELECT topic_id, UPPER(TRIM(stock_code)) AS norm_code, COUNT(*) AS cnt
          FROM stock_mentions
          {where}
          GROUP BY topic_id, norm_code
          HAVING COUNT(*) > 1
        ) t
        """,
        params,
    )
    row = cursor.fetchone() or (0, 0)
    return int(row[0] or 0), int(row[1] or 0)


def _collect_duplicate_ids(cursor: sqlite3.Cursor, topic_id: Optional[str]) -> List[int]:
    where = ""
    params: List[object] = []
    if topic_id:
        where = "WHERE topic_id = ?"
        params.append(topic_id)

    cursor.execute(
        f"""
        WITH ranked AS (
          SELECT
            id,
            ROW_NUMBER() OVER (
              PARTITION BY topic_id, UPPER(TRIM(stock_code))
              ORDER BY COALESCE(mention_time, '') DESC, id DESC
            ) AS rn
          FROM stock_mentions
          {where}
        )
        SELECT id
        FROM ranked
        WHERE rn > 1
        """,
        params,
    )
    return [int(r[0]) for r in cursor.fetchall()]


def _count_orphan_performance(cursor: sqlite3.Cursor) -> int:
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM mention_performance mp
        LEFT JOIN stock_mentions sm ON sm.id = mp.mention_id
        WHERE sm.id IS NULL
        """
    )
    return int((cursor.fetchone() or [0])[0] or 0)


def clean_db(db_path: Path, topic_id: Optional[str], apply: bool) -> CleanStats:
    stats = CleanStats(db_path=str(db_path))
    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("PRAGMA busy_timeout=30000")

    try:
        cursor.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='stock_mentions'"
        )
        if int((cursor.fetchone() or [0])[0] or 0) == 0:
            return stats

        where = ""
        params: List[object] = []
        if topic_id:
            where = "WHERE topic_id = ?"
            params.append(topic_id)

        cursor.execute(f"SELECT COUNT(*) FROM stock_mentions {where}", params)
        stats.total_before = int((cursor.fetchone() or [0])[0] or 0)
        stats.distinct_pairs_before = _count_distinct_pairs(cursor, topic_id)
        dup_rows, dup_groups = _count_duplicate_rows_and_groups(cursor, topic_id)
        stats.duplicate_rows_before = dup_rows
        stats.duplicate_groups_before = dup_groups

        cursor.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='mention_performance'"
        )
        has_perf = int((cursor.fetchone() or [0])[0] or 0) > 0
        if has_perf:
            stats.orphan_perf_before = _count_orphan_performance(cursor)

        if not apply:
            return stats

        cursor.execute(
            """
            UPDATE stock_mentions
            SET stock_code = UPPER(TRIM(stock_code))
            WHERE stock_code != UPPER(TRIM(stock_code))
            """
        )
        stats.normalized_codes_updated = int(cursor.rowcount or 0)

        duplicate_ids = _collect_duplicate_ids(cursor, topic_id)
        if duplicate_ids:
            chunk = 900
            for i in range(0, len(duplicate_ids), chunk):
                part = duplicate_ids[i : i + chunk]
                placeholders = ",".join(["?"] * len(part))
                cursor.execute(
                    f"DELETE FROM stock_mentions WHERE id IN ({placeholders})",
                    part,
                )
                stats.removed_duplicate_rows += int(cursor.rowcount or 0)

        if has_perf:
            cursor.execute(
                """
                DELETE FROM mention_performance
                WHERE mention_id NOT IN (SELECT id FROM stock_mentions)
                """
            )
            stats.removed_orphan_perf = int(cursor.rowcount or 0)

        cursor.execute(f"SELECT COUNT(*) FROM stock_mentions {where}", params)
        stats.total_after = int((cursor.fetchone() or [0])[0] or 0)
        stats.distinct_pairs_after = _count_distinct_pairs(cursor, topic_id)
        dup_rows_after, _ = _count_duplicate_rows_and_groups(cursor, topic_id)
        stats.duplicate_rows_after = dup_rows_after

        conn.commit()
        return stats
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _resolve_target_dbs(args: argparse.Namespace) -> List[Path]:
    if args.db:
        path = Path(args.db).expanduser().resolve()
        return [path]

    mgr = get_db_path_manager()
    groups = mgr.list_all_groups()

    if args.group_id:
        gid = str(args.group_id).strip()
        targets = [Path(item["topics_db"]) for item in groups if str(item.get("group_id")) == gid]
        return targets

    if args.all_groups:
        return [Path(item["topics_db"]) for item in groups]

    return []


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="去重 stock_mentions 并清理 mention_performance 孤儿记录")
    parser.add_argument("--db", help="指定单个 topics DB 路径")
    parser.add_argument("--group-id", help="指定群组 ID（从 output/databases 自动定位 DB）")
    parser.add_argument("--all-groups", action="store_true", help="处理所有本地群组 topics DB")
    parser.add_argument("--topic-id", help="仅清洗指定 topic_id（如 55188581551414214）")
    parser.add_argument("--apply", action="store_true", help="执行真实清洗（默认仅 dry-run）")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    target_dbs = _resolve_target_dbs(args)
    if not target_dbs:
        print("未找到可处理的数据库。请使用 --db / --group-id / --all-groups。")
        return 1

    print(f"模式: {'APPLY' if args.apply else 'DRY-RUN'}")
    if args.topic_id:
        print(f"范围: 仅 topic_id={args.topic_id}")
    print(f"目标库数量: {len(target_dbs)}")

    total_removed_dup = 0
    total_removed_orphans = 0

    for db in target_dbs:
        if not db.exists():
            print(f"\n[SKIP] {db} (不存在)")
            continue

        stats = clean_db(db, topic_id=args.topic_id, apply=args.apply)
        print(f"\n[DB] {stats.db_path}")
        print(f"  stock_mentions: {stats.total_before} -> {stats.total_after if args.apply else stats.total_before}")
        print(f"  唯一(topic_id+stock_code): {stats.distinct_pairs_before} -> {stats.distinct_pairs_after if args.apply else stats.distinct_pairs_before}")
        print(f"  重复组: {stats.duplicate_groups_before}")
        print(f"  重复行(可删): {stats.duplicate_rows_before}")
        if args.apply:
            print(f"  规范化 code 更新: {stats.normalized_codes_updated}")
            print(f"  已删除重复行: {stats.removed_duplicate_rows}")
            print(f"  残余重复行: {stats.duplicate_rows_after}")
        print(f"  mention_performance 孤儿(可删): {stats.orphan_perf_before}")
        if args.apply:
            print(f"  已删除孤儿: {stats.removed_orphan_perf}")

        total_removed_dup += stats.removed_duplicate_rows
        total_removed_orphans += stats.removed_orphan_perf

    if args.apply:
        print("\n== 清洗完成 ==")
        print(f"总删除重复行: {total_removed_dup}")
        print(f"总删除 performance 孤儿: {total_removed_orphans}")
        print("下一步建议：重启服务，让 uq_sm_topic_stock 唯一索引自动创建。")
    else:
        print("\n== Dry-run 完成 ==")
        print("如需执行清洗，请追加 --apply。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
