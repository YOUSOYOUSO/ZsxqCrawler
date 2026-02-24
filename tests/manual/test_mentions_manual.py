import sqlite3
import os
from modules.shared.db_path_manager import get_db_path_manager
from modules.shared.stock_exclusion import is_excluded_stock
from modules.shared.group_scan_filter import filter_groups

manager = get_db_path_manager()
all_groups = manager.list_all_groups()
groups = filter_groups(all_groups).get("included_groups", []) or []

for g in groups:
    db_path = g['topics_db']
    group_id = g['group_id']
    if not os.path.exists(db_path): continue
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        cursor.execute('''
            SELECT sm.stock_code, sm.stock_name, mp.return_5d
            FROM stock_mentions sm
            LEFT JOIN mention_performance mp ON sm.id = mp.mention_id
        ''')
        for row in cursor.fetchall():
            code, name, ret = row
            if is_excluded_stock(code, name): continue
            print(f"Group {group_id}: {code} ({name}) - return_5d: {ret}")
    except Exception as e:
        print(f"Error in {group_id}: {e}")
    conn.close()
