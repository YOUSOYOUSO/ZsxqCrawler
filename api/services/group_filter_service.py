from typing import Any, Dict, List


def apply_group_scan_filter(groups: List[Dict[str, Any]]) -> Dict[str, Any]:
    """统一应用白黑名单过滤，供全区任务与调度复用。"""
    from modules.shared.group_scan_filter import filter_groups

    filtered = filter_groups(groups)
    cfg = filtered.get("config", {}) or {}
    return {
        "all_groups": groups,
        "included_groups": filtered.get("included_groups", []) or [],
        "excluded_groups": filtered.get("excluded_groups", []) or [],
        "reason_counts": filtered.get("reason_counts", {}) or {},
        "default_action": str(cfg.get("default_action", "include")),
    }


def format_group_filter_summary(
    all_groups: List[Dict[str, Any]],
    included_groups: List[Dict[str, Any]],
    excluded_groups: List[Dict[str, Any]],
    reason_counts: Dict[str, Any],
    default_action: str,
) -> List[str]:
    """将过滤结果格式化为日志行。"""
    lines = [
        f"📋 共发现 {len(all_groups)} 个群组",
        f"⚙️ 过滤策略: 未配置群组默认{'纳入' if default_action == 'include' else '排除'}",
        f"🧹 过滤后纳入 {len(included_groups)}/{len(all_groups)} 个群组",
    ]
    if reason_counts:
        lines.append(f"📌 命中统计: {reason_counts}")
    if excluded_groups:
        preview = "，".join(
            f"{g.get('group_id')}({g.get('scan_filter_reason', 'unknown')})"
            for g in excluded_groups[:20]
        )
        suffix = " ..." if len(excluded_groups) > 20 else ""
        lines.append(f"🚫 已排除: {preview}{suffix}")
    return lines

