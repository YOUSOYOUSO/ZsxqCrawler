from modules.shared.db_path_manager import get_db_path_manager
from modules.shared.group_scan_filter import filter_groups
import json

manager = get_db_path_manager()
all_groups = manager.list_all_groups()
groups = filter_groups(all_groups).get("included_groups", []) or []

print("Total filtered groups:", len(groups))
for g in groups:
    print(g['group_id'], g.get('group_name'))
