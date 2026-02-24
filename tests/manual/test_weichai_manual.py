import sqlite3
import os
from modules.shared.db_path_manager import get_db_path_manager

manager = get_db_path_manager()
all_groups = manager.list_all_groups()

for g in all_groups:
    if g['group_id'] != '51122424541244': continue
    db_path = g['topics_db']
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT sm.stock_code, sm.stock_name, sm.mention_date, mp.price_at_mention, mp.return_1d, mp.return_5d
        FROM stock_mentions sm
        LEFT JOIN mention_performance mp ON sm.id = mp.mention_id
        WHERE sm.stock_code='000338.SZ'
    ''')
    for row in cursor.fetchall():
        print(f"潍柴动力: date={row[2]}, price_at_mention={row[3]}, return_1d={row[4]}, return_5d={row[5]}")
    conn.close()
