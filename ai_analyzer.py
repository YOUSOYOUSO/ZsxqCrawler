"""
AI 分析模块 — 基于 DeepSeek API 的股票智能分析
使用 OpenAI 兼容格式，通过 httpx 直接调用
"""

import json
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import httpx

try:
    import tomllib
except ImportError:
    try:
        import tomli as tomllib  # type: ignore
    except ImportError:
        tomllib = None  # type: ignore

from logger_config import log_info, log_warning, log_error

BEIJING_TZ = timezone(timedelta(hours=8))

# ========== Prompt 模板 ==========

STOCK_ANALYSIS_PROMPT = """你是一位专业的A股投资分析师。请根据以下股票的历史提及数据和表现，撰写一份简明的分析报告。

## 股票信息
- 股票名称: {stock_name}
- 股票代码: {stock_code}
- 总被提及次数: {total_mentions}
- 5日胜率: {win_rate}
- 5日平均收益: {avg_return}

## 历史提及事件（按时间排序）
{events_text}

## 分析要求
请从以下几个角度进行分析：
1. **提及规律** — 该股票被提及的频率和时间分布是否有规律？
2. **收益表现** — 被提及后的收益表现如何？是否存在稳定的正收益？
3. **关键事件** — 哪些提及后出现了显著涨幅或回撤？可能的原因？
4. **风险提示** — 基于历史数据的主要风险
5. **综合评估** — 该信源对此股票的推荐可信度如何？

请用 Markdown 格式输出，保持简洁专业，不超过500字。
"""

DAILY_BRIEF_PROMPT = """你是一位专业的投资研究助理。请根据以下近期股票信号数据，撰写一份简明的每日投资观察简报。

## 近期高频信号（近{lookback_days}天内被频繁提及的股票）
{signals_text}

## 整体统计
- 总提及数: {total_mentions}
- 涉及股票数: {unique_stocks}
- 整体5日胜率: {overall_win_rate}

## 简报要求
请从以下角度撰写：
1. **市场热点** — 近期最受关注的方向和个股
2. **重点关注** — 哪些信号值得重点跟踪？为什么？
3. **风险提醒** — 需要警惕的异常信号或过热个股
4. **操作建议** — 基于数据的理性建议（不构成投资建议）

请用 Markdown 格式输出，保持简洁（不超过600字）。标记重要观点。
"""

CONSENSUS_PROMPT = """你是一位专业的量化分析师。请对比分析以下热门股票的数据，寻找市场共识和分歧。

## 热门股票数据
{stocks_text}

## 分析要求
1. **共识方向** — 哪些股票多次被提及且表现一致？看多/看空共识如何？
2. **分歧标的** — 哪些股票表现分化？可能的原因？
3. **板块趋势** — 从股票分布看，市场关注的主要板块
4. **性价比排序** — 综合胜率和收益，哪些标的性价比最高？
5. **要点总结** — 3-5条核心结论

请用 Markdown 格式输出，不超过500字。用表格展示对比数据。
"""


class AIAnalyzer:
    """DeepSeek AI 分析器"""

    def __init__(self, db_path: str = None, group_id: str = None):
        self.group_id = group_id
        self.db_path = db_path
        self._api_key: Optional[str] = None
        self._base_url: str = "https://api.deepseek.com"
        self._model: str = "deepseek-chat"
        self._load_config()

        # 初始化 AI 缓存表
        if self.db_path:
            self._init_ai_tables()

    def _load_config(self):
        """从 config.toml 或环境变量加载 AI 配置"""
        # 优先环境变量
        self._api_key = os.environ.get("DEEPSEEK_API_KEY")
        base = os.environ.get("DEEPSEEK_BASE_URL")
        if base:
            self._base_url = base
        model = os.environ.get("DEEPSEEK_MODEL")
        if model:
            self._model = model

        if self._api_key:
            return

        # 尝试从 config.toml 读取
        if tomllib is None:
            return

        config_paths = ["config.toml", "../config.toml"]
        for path in config_paths:
            if os.path.exists(path):
                try:
                    with open(path, 'rb') as f:
                        config = tomllib.load(f)
                    ai_cfg = config.get('ai', {})
                    self._api_key = ai_cfg.get('api_key', '')
                    if ai_cfg.get('base_url'):
                        self._base_url = ai_cfg['base_url']
                    if ai_cfg.get('model'):
                        self._model = ai_cfg['model']
                    break
                except Exception as e:
                    log_warning(f"读取AI配置失败: {e}")

    @property
    def is_configured(self) -> bool:
        return bool(self._api_key and self._api_key.strip())

    def get_config_status(self) -> Dict[str, Any]:
        return {
            'configured': self.is_configured,
            'model': self._model,
            'base_url': self._base_url,
            'api_key_preview': f"{self._api_key[:8]}...{self._api_key[-4:]}" if self._api_key and len(self._api_key) > 12 else ('已配置' if self._api_key else '未配置')
        }

    def update_config(self, api_key: str, base_url: str = None, model: str = None):
        """更新 AI 配置到 config.toml"""
        self._api_key = api_key
        if base_url:
            self._base_url = base_url
        if model:
            self._model = model

        # 读取现有 config.toml
        config_path = "config.toml"
        content = ""
        if os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                content = f.read()

        # 检查是否已有 [ai] 段
        if '[ai]' in content:
            # 替换已有的 [ai] 段
            import re
            pattern = r'\[ai\].*?(?=\n\[|\Z)'
            replacement = f'[ai]\napi_key = "{api_key}"\nmodel = "{self._model}"\nbase_url = "{self._base_url}"\n'
            content = re.sub(pattern, replacement, content, flags=re.DOTALL)
        else:
            # 追加 [ai] 段
            content += f'\n[ai]\napi_key = "{api_key}"\nmodel = "{self._model}"\nbase_url = "{self._base_url}"\n'

        with open(config_path, 'w', encoding='utf-8') as f:
            f.write(content)

        log_info("AI 配置已更新")

    def _init_ai_tables(self):
        """初始化 AI 缓存数据表"""
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ai_summaries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                summary_type TEXT NOT NULL,
                target_key TEXT,
                content TEXT NOT NULL,
                model TEXT,
                tokens_used INTEGER,
                created_at TEXT DEFAULT (datetime('now', 'localtime')),
                UNIQUE(summary_type, target_key)
            )
        ''')
        conn.commit()
        conn.close()

    # ========== DeepSeek API 调用 ==========

    def _call_deepseek(self, system_prompt: str, user_prompt: str, max_tokens: int = 2000) -> Dict[str, Any]:
        """调用 DeepSeek Chat API"""
        if not self.is_configured:
            return {'error': '未配置 DeepSeek API Key，请在设置中配置', 'content': None}

        url = f"{self._base_url.rstrip('/')}/chat/completions"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._api_key}"
        }
        payload = {
            "model": self._model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "max_tokens": max_tokens,
            "temperature": 0.7,
            "stream": False
        }

        try:
            with httpx.Client(timeout=60.0) as client:
                resp = client.post(url, headers=headers, json=payload)
                resp.raise_for_status()
                data = resp.json()

            content = data['choices'][0]['message']['content']
            tokens = data.get('usage', {}).get('total_tokens', 0)
            return {'content': content, 'tokens_used': tokens, 'model': self._model}

        except httpx.HTTPStatusError as e:
            error_msg = f"API 请求失败 ({e.response.status_code})"
            try:
                detail = e.response.json()
                error_msg += f": {detail.get('error', {}).get('message', str(detail))}"
            except Exception:
                error_msg += f": {e.response.text[:200]}"
            log_error(error_msg)
            return {'error': error_msg, 'content': None}

        except httpx.TimeoutException:
            log_error("DeepSeek API 请求超时")
            return {'error': 'API 请求超时，请稍后重试', 'content': None}

        except Exception as e:
            log_error(f"DeepSeek API 调用异常: {e}")
            return {'error': f'API 调用异常: {str(e)}', 'content': None}

    # ========== 缓存管理 ==========

    def _get_cached(self, summary_type: str, target_key: str, max_age_hours: int = 24) -> Optional[Dict]:
        """获取缓存的 AI 分析结果"""
        if not self.db_path:
            return None

        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute('''
            SELECT * FROM ai_summaries
            WHERE summary_type = ? AND target_key = ?
            ORDER BY created_at DESC LIMIT 1
        ''', (summary_type, target_key))

        row = cursor.fetchone()
        conn.close()

        if not row:
            return None

        # 检查是否过期
        created = datetime.strptime(row['created_at'], '%Y-%m-%d %H:%M:%S')
        if datetime.now() - created > timedelta(hours=max_age_hours):
            return None

        return dict(row)

    def _save_cache(self, summary_type: str, target_key: str, content: str,
                    model: str, tokens_used: int):
        """保存 AI 分析结果到缓存"""
        if not self.db_path:
            return

        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO ai_summaries
            (summary_type, target_key, content, model, tokens_used, created_at)
            VALUES (?, ?, ?, ?, ?, datetime('now', 'localtime'))
        ''', (summary_type, target_key, content, model, tokens_used))
        conn.commit()
        conn.close()

    # ========== 分析功能 ==========

    def analyze_stock(self, stock_code: str, force: bool = False) -> Dict[str, Any]:
        """
        分析单只股票：汇总提及事件 + AI 生成报告

        Args:
            stock_code: 如 '300579.SZ'
            force: 是否强制重新分析（忽略缓存）
        """
        # 检查缓存
        if not force:
            cached = self._get_cached('stock', stock_code)
            if cached:
                return {
                    'stock_code': stock_code,
                    'content': cached['content'],
                    'model': cached['model'],
                    'tokens_used': cached['tokens_used'],
                    'created_at': cached['created_at'],
                    'from_cache': True
                }

        # 获取股票数据
        from stock_analyzer import StockAnalyzer
        analyzer = StockAnalyzer(group_id=self.group_id)
        events_data = analyzer.get_stock_events(stock_code)

        if not events_data or events_data['total_mentions'] == 0:
            return {'error': f'未找到 {stock_code} 的提及数据', 'content': None}

        # 构建事件文本
        events_text = ""
        for e in events_data['events'][:20]:  # 最多20条
            ret_str = ""
            if e.get('return_1d') is not None:
                ret_str = f"T+1: {e['return_1d']}% | T+5: {e.get('return_5d', '—')}% | T+10: {e.get('return_10d', '—')}%"
            ctx = (e.get('context_snippet') or '')[:80]
            events_text += f"- [{e['mention_date']}] {ctx}... → {ret_str}\n"

        prompt = STOCK_ANALYSIS_PROMPT.format(
            stock_name=events_data['stock_name'],
            stock_code=stock_code,
            total_mentions=events_data['total_mentions'],
            win_rate=f"{events_data['win_rate_5d']}%" if events_data.get('win_rate_5d') is not None else '暂无',
            avg_return=f"{events_data['avg_return_5d']}%" if events_data.get('avg_return_5d') is not None else '暂无',
            events_text=events_text
        )

        result = self._call_deepseek(
            system_prompt="你是一位专业的A股投资分析师，擅长从舆情数据中提取投资信号。",
            user_prompt=prompt
        )

        if result.get('error'):
            return result

        # 保存缓存
        self._save_cache('stock', stock_code, result['content'],
                         result['model'], result['tokens_used'])

        return {
            'stock_code': stock_code,
            'stock_name': events_data['stock_name'],
            'content': result['content'],
            'model': result['model'],
            'tokens_used': result['tokens_used'],
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'from_cache': False
        }

    def generate_daily_brief(self, lookback_days: int = 7, force: bool = False) -> Dict[str, Any]:
        """生成每日投资简报"""
        today = datetime.now(BEIJING_TZ).strftime('%Y-%m-%d')
        cache_key = f"daily_{today}"

        if not force:
            cached = self._get_cached('daily', cache_key, max_age_hours=12)
            if cached:
                return {
                    'content': cached['content'],
                    'model': cached['model'],
                    'tokens_used': cached['tokens_used'],
                    'created_at': cached['created_at'],
                    'from_cache': True
                }

        from stock_analyzer import StockAnalyzer
        analyzer = StockAnalyzer(group_id=self.group_id)

        signals = analyzer.get_signals(lookback_days=lookback_days, min_mentions=1)
        stats = analyzer.get_summary_stats()

        if not signals:
            return {'error': '近期无信号数据', 'content': None}

        signals_text = ""
        for s in signals[:15]:
            wr = f"{s['historical_win_rate']}%" if s.get('historical_win_rate') is not None else '—'
            avg = f"{s['historical_avg_return']}%" if s.get('historical_avg_return') is not None else '—'
            ctx = (s.get('recent_contexts') or '')[:100]
            signals_text += f"- **{s['stock_name']}** ({s['stock_code']}): 近期提及{s['recent_mentions']}次, 历史胜率{wr}, 均收益{avg}\n  最近上下文: {ctx}\n"

        prompt = DAILY_BRIEF_PROMPT.format(
            lookback_days=lookback_days,
            signals_text=signals_text,
            total_mentions=stats.get('total_mentions', 0),
            unique_stocks=stats.get('unique_stocks', 0),
            overall_win_rate=f"{stats['overall_win_rate_5d']}%" if stats.get('overall_win_rate_5d') is not None else '暂无'
        )

        result = self._call_deepseek(
            system_prompt="你是一位专业的投资研究助理，擅长撰写清晰简洁的每日投资观察简报。",
            user_prompt=prompt
        )

        if result.get('error'):
            return result

        self._save_cache('daily', cache_key, result['content'],
                         result['model'], result['tokens_used'])

        return {
            'date': today,
            'content': result['content'],
            'model': result['model'],
            'tokens_used': result['tokens_used'],
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'from_cache': False
        }

    def analyze_consensus(self, top_n: int = 10, force: bool = False) -> Dict[str, Any]:
        """对比分析热门股票，寻找市场共识"""
        today = datetime.now(BEIJING_TZ).strftime('%Y-%m-%d')
        cache_key = f"consensus_{today}"

        if not force:
            cached = self._get_cached('consensus', cache_key, max_age_hours=12)
            if cached:
                return {
                    'content': cached['content'],
                    'model': cached['model'],
                    'tokens_used': cached['tokens_used'],
                    'created_at': cached['created_at'],
                    'from_cache': True
                }

        from stock_analyzer import StockAnalyzer
        analyzer = StockAnalyzer(group_id=self.group_id)

        ranking = analyzer.get_win_rate_ranking(min_mentions=2, limit=top_n)

        if not ranking:
            return {'error': '暂无足够的股票数据进行共识分析', 'content': None}

        stocks_text = "| 股票 | 代码 | 提及次数 | 胜率 | 平均收益 | 最大涨幅 | 最大回撤 |\n"
        stocks_text += "|---|---|---|---|---|---|---|\n"
        for r in ranking:
            stocks_text += (
                f"| {r['stock_name']} | {r['stock_code']} | {r['total_mentions']} | "
                f"{r['win_rate']}% | {r['avg_return']}% | {r['best_max_return']}% | "
                f"{r['worst_drawdown']}% |\n"
            )

        prompt = CONSENSUS_PROMPT.format(stocks_text=stocks_text)

        result = self._call_deepseek(
            system_prompt="你是一位专业的量化分析师，擅长从多维度数据中发现市场共识和投资机会。",
            user_prompt=prompt
        )

        if result.get('error'):
            return result

        self._save_cache('consensus', cache_key, result['content'],
                         result['model'], result['tokens_used'])

        return {
            'content': result['content'],
            'model': result['model'],
            'tokens_used': result['tokens_used'],
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'from_cache': False,
            'stocks_analyzed': len(ranking)
        }

    def get_history(self, summary_type: str = None, limit: int = 20) -> List[Dict]:
        """获取 AI 分析历史记录"""
        if not self.db_path:
            return []

        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        if summary_type:
            cursor.execute('''
                SELECT id, summary_type, target_key, model, tokens_used, created_at,
                       SUBSTR(content, 1, 200) as preview
                FROM ai_summaries
                WHERE summary_type = ?
                ORDER BY created_at DESC LIMIT ?
            ''', (summary_type, limit))
        else:
            cursor.execute('''
                SELECT id, summary_type, target_key, model, tokens_used, created_at,
                       SUBSTR(content, 1, 200) as preview
                FROM ai_summaries
                ORDER BY created_at DESC LIMIT ?
            ''', (limit,))

        items = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return items

    def get_history_detail(self, summary_id: int) -> Optional[Dict]:
        """获取某条历史分析的完整内容"""
        if not self.db_path:
            return None

        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM ai_summaries WHERE id = ?', (summary_id,))
        row = cursor.fetchone()
        conn.close()

        return dict(row) if row else None
