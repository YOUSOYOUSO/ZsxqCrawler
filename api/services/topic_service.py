from __future__ import annotations

import gc
import os
import sqlite3
import time
from typing import Any, Dict, Optional

import requests
from fastapi import HTTPException

from modules.accounts.accounts_sql_manager import get_accounts_sql_manager
from modules.shared.db_path_manager import get_db_path_manager
from modules.zsxq.zsxq_interactive_crawler import ZSXQInteractiveCrawler, load_config


class TopicService:
    TOPIC_RELATED_TABLES = [
        "user_liked_emojis",
        "like_emojis",
        "likes",
        "images",
        "comments",
        "answers",
        "questions",
        "articles",
        "talks",
        "topic_files",
        "topic_tags",
    ]

    def _resolve_cookie_for_group(self, group_id: str) -> str:
        manager = get_accounts_sql_manager()
        account = manager.get_account_for_group(group_id, mask_cookie=False)
        if account and account.get("cookie"):
            return str(account["cookie"]).strip()

        first = manager.get_first_account(mask_cookie=False)
        if first and first.get("cookie"):
            return str(first["cookie"]).strip()

        cfg = load_config() or {}
        return str((cfg.get("auth", {}) or {}).get("cookie", "")).strip()

    def _build_crawler(self, group_id: str, log_callback=None) -> ZSXQInteractiveCrawler:
        cookie = self._resolve_cookie_for_group(group_id)
        if not cookie or cookie == "your_cookie_here":
            raise HTTPException(status_code=400, detail="未找到可用Cookie，请先在账号管理或 config/app.toml 中配置")

        db_path = get_db_path_manager().get_topics_db_path(group_id)
        return ZSXQInteractiveCrawler(cookie, group_id, db_path, log_callback)

    def _build_default_crawler(self, log_callback=None) -> ZSXQInteractiveCrawler:
        cfg = load_config() or {}
        auth = cfg.get("auth", {}) or {}
        cookie = str(auth.get("cookie", "")).strip()
        group_id = str(auth.get("group_id", "")).strip()
        if not cookie or not group_id or cookie == "your_cookie_here" or group_id == "your_group_id_here":
            raise HTTPException(status_code=400, detail="请先在 config/app.toml 中配置Cookie和群组ID")
        db_path = get_db_path_manager().get_topics_db_path(group_id)
        return ZSXQInteractiveCrawler(cookie, group_id, db_path, log_callback)

    def clear_topic_database(self, group_id: str) -> Dict[str, Any]:
        db_path = get_db_path_manager().get_topics_db_path(group_id)
        delete_stats = {
            "topics_deleted": 0,
            "mentions_deleted": 0,
            "performances_deleted": 0,
            "cache_invalidated": False,
        }

        if not os.path.exists(db_path):
            return {
                "message": f"群组 {group_id} 的话题数据库不存在",
                "group_id": group_id,
                "deleted": delete_stats,
            }

        try:
            conn = sqlite3.connect(db_path, timeout=10)
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM topics")
            delete_stats["topics_deleted"] = int((cur.fetchone() or [0])[0] or 0)
            for table, key in (("stock_mentions", "mentions_deleted"), ("mention_performance", "performances_deleted")):
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    delete_stats[key] = int((cur.fetchone() or [0])[0] or 0)
                except Exception:
                    pass
            conn.close()
        except Exception:
            pass

        try:
            crawler = self._build_crawler(group_id)
            if getattr(crawler, "db", None):
                crawler.db.close()
            if getattr(crawler, "file_downloader", None) and getattr(crawler.file_downloader, "file_db", None):
                crawler.file_downloader.file_db.close()
        except Exception:
            pass

        gc.collect()
        time.sleep(0.5)

        try:
            os.remove(db_path)
        except PermissionError:
            raise HTTPException(status_code=500, detail="文件被占用，无法删除数据库文件。请稍后重试。")

        try:
            from app.runtime.image_cache_manager import clear_group_cache_manager, get_image_cache_manager

            cache_manager = get_image_cache_manager(group_id)
            cache_manager.clear_cache()
            clear_group_cache_manager(group_id)
        except Exception:
            pass

        try:
            from modules.analyzers.global_analyzer import get_global_analyzer

            get_global_analyzer().invalidate_cache()
            delete_stats["cache_invalidated"] = True
        except Exception:
            pass

        return {
            "message": f"群组 {group_id} 的话题数据库和图片缓存已删除",
            "group_id": group_id,
            "deleted": delete_stats,
        }

    def get_topics(self, page: int, per_page: int, search: Optional[str]) -> Dict[str, Any]:
        crawler = self._build_default_crawler()
        offset = (page - 1) * per_page

        if search:
            query = """
                SELECT topic_id, title, create_time, likes_count, comments_count, reading_count
                FROM topics
                WHERE title LIKE ?
                ORDER BY create_time DESC
                LIMIT ? OFFSET ?
            """
            params = (f"%{search}%", per_page, offset)
            crawler.db.cursor.execute("SELECT COUNT(*) FROM topics WHERE title LIKE ?", (f"%{search}%",))
        else:
            query = """
                SELECT topic_id, title, create_time, likes_count, comments_count, reading_count
                FROM topics
                ORDER BY create_time DESC
                LIMIT ? OFFSET ?
            """
            params = (per_page, offset)
            crawler.db.cursor.execute("SELECT COUNT(*) FROM topics")

        total = crawler.db.cursor.fetchone()[0]
        crawler.db.cursor.execute(query, params)
        rows = crawler.db.cursor.fetchall()

        return {
            "topics": [
                {
                    "topic_id": str(row[0]) if row[0] is not None else None,
                    "title": row[1],
                    "create_time": row[2],
                    "likes_count": row[3],
                    "comments_count": row[4],
                    "reading_count": row[5],
                }
                for row in rows
            ],
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total": total,
                "pages": (total + per_page - 1) // per_page,
            },
        }

    def get_topic_detail(self, topic_id: int, group_id: str) -> Dict[str, Any]:
        crawler = self._build_crawler(group_id)
        detail = crawler.db.get_topic_detail(topic_id)
        if not detail:
            raise HTTPException(status_code=404, detail="话题不存在")
        return detail

    def refresh_topic(self, topic_id: int, group_id: str) -> Dict[str, Any]:
        crawler = self._build_crawler(group_id)
        url = f"https://api.zsxq.com/v2/topics/{topic_id}/info"
        response = requests.get(url, headers=crawler.get_stealth_headers(), timeout=30)

        if response.status_code != 200:
            return {"success": False, "message": f"API请求失败: {response.status_code}"}

        data = response.json()
        if not data.get("succeeded") or not data.get("resp_data"):
            return {"success": False, "message": "API返回数据格式错误"}

        topic_data = data["resp_data"]["topic"]
        success = crawler.db.update_topic_stats(topic_data)
        if not success:
            return {"success": False, "message": "话题不存在或更新失败"}

        crawler.db.conn.commit()
        return {
            "success": True,
            "message": "话题信息已更新",
            "updated_data": {
                "likes_count": topic_data.get("likes_count", 0),
                "comments_count": topic_data.get("comments_count", 0),
                "reading_count": topic_data.get("reading_count", 0),
                "readers_count": topic_data.get("readers_count", 0),
            },
        }

    def fetch_more_comments(self, topic_id: int, group_id: str) -> Dict[str, Any]:
        crawler = self._build_crawler(group_id)
        topic_detail = crawler.db.get_topic_detail(topic_id)
        if not topic_detail:
            raise HTTPException(status_code=404, detail="话题不存在")

        comments_count = topic_detail.get("comments_count", 0)
        if comments_count <= 8:
            return {
                "success": True,
                "message": f"话题只有 {comments_count} 条评论，无需获取更多",
                "comments_fetched": 0,
            }

        try:
            additional_comments = crawler.fetch_all_comments(topic_id, comments_count)
            if additional_comments:
                crawler.db.import_additional_comments(topic_id, additional_comments)
                crawler.db.conn.commit()
                return {
                    "success": True,
                    "message": f"成功获取并导入 {len(additional_comments)} 条评论",
                    "comments_fetched": len(additional_comments),
                }
            return {
                "success": False,
                "message": "获取评论失败，可能是权限限制或网络问题",
                "comments_fetched": 0,
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"获取评论时出错: {e}",
                "comments_fetched": 0,
            }

    def delete_single_topic(self, topic_id: int, group_id: int) -> Dict[str, Any]:
        crawler = self._build_crawler(str(group_id))
        try:
            crawler.db.cursor.execute("SELECT COUNT(*) FROM topics WHERE topic_id = ? AND group_id = ?", (topic_id, group_id))
            exists = crawler.db.cursor.fetchone()[0] > 0
            if not exists:
                return {"success": False, "message": "话题不存在"}

            for table in self.TOPIC_RELATED_TABLES:
                crawler.db.cursor.execute(f"DELETE FROM {table} WHERE topic_id = ?", (topic_id,))
            crawler.db.cursor.execute("DELETE FROM topics WHERE topic_id = ? AND group_id = ?", (topic_id, group_id))
            deleted = crawler.db.cursor.rowcount
            crawler.db.conn.commit()
            return {"success": True, "deleted_topic_id": topic_id, "deleted": deleted > 0}
        except Exception:
            crawler.db.conn.rollback()
            raise

    def fetch_single_topic(self, group_id: str, topic_id: int, fetch_comments: bool = True) -> Dict[str, Any]:
        crawler = self._build_crawler(group_id)
        url = f"https://api.zsxq.com/v2/topics/{topic_id}/info"
        response = requests.get(url, headers=crawler.get_stealth_headers(), timeout=30)

        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail="API请求失败")

        data = response.json()
        if not data.get("succeeded") or not data.get("resp_data"):
            raise HTTPException(status_code=400, detail="API返回失败")

        topic = (data.get("resp_data", {}) or {}).get("topic", {}) or {}
        if not topic:
            raise HTTPException(status_code=404, detail="未获取到有效话题数据")

        topic_group_id = str((topic.get("group") or {}).get("group_id", ""))
        if topic_group_id and topic_group_id != str(group_id):
            raise HTTPException(status_code=400, detail="该话题不属于当前群组")

        crawler.db.cursor.execute("SELECT topic_id FROM topics WHERE topic_id = ?", (topic_id,))
        existed = crawler.db.cursor.fetchone() is not None

        crawler.db.import_topic_data(topic)
        crawler.db.conn.commit()

        comments_fetched = 0
        if fetch_comments:
            comments_count = topic.get("comments_count", 0) or 0
            if comments_count > 0:
                try:
                    additional_comments = crawler.fetch_all_comments(topic_id, comments_count)
                    if additional_comments:
                        crawler.db.import_additional_comments(topic_id, additional_comments)
                        crawler.db.conn.commit()
                        comments_fetched = len(additional_comments)
                except Exception:
                    pass

        return {
            "success": True,
            "topic_id": str(topic_id) if topic_id is not None else None,
            "group_id": int(group_id),
            "imported": "updated" if existed else "created",
            "comments_fetched": comments_fetched,
        }

    def get_group_tags(self, group_id: str) -> Dict[str, Any]:
        crawler = self._build_crawler(group_id)
        tags = crawler.db.get_tags_by_group(int(group_id))
        return {"tags": tags, "total": len(tags)}

    def get_topics_by_tag(self, group_id: int, tag_id: int, page: int = 1, per_page: int = 20) -> Dict[str, Any]:
        crawler = self._build_crawler(str(group_id))
        crawler.db.cursor.execute("SELECT COUNT(*) FROM tags WHERE tag_id = ? AND group_id = ?", (tag_id, group_id))
        if crawler.db.cursor.fetchone()[0] == 0:
            raise HTTPException(status_code=404, detail="标签在该群组中不存在")
        return crawler.db.get_topics_by_tag(tag_id, page, per_page)

    def get_group_topics(self, group_id: int, page: int, per_page: int, search: Optional[str]) -> Dict[str, Any]:
        crawler = self._build_crawler(str(group_id))
        offset = (page - 1) * per_page

        if search:
            query = """
                SELECT
                    t.topic_id, t.title, t.create_time, t.likes_count, t.comments_count,
                    t.reading_count, t.type, t.digested, t.sticky,
                    q.text as question_text,
                    a.text as answer_text,
                    tk.text as talk_text,
                    u.user_id, u.name, u.avatar_url, t.imported_at
                FROM topics t
                LEFT JOIN questions q ON t.topic_id = q.topic_id
                LEFT JOIN answers a ON t.topic_id = a.topic_id
                LEFT JOIN talks tk ON t.topic_id = tk.topic_id
                LEFT JOIN users u ON tk.owner_user_id = u.user_id
                WHERE t.group_id = ? AND (t.title LIKE ? OR q.text LIKE ? OR tk.text LIKE ?)
                ORDER BY t.create_time DESC
                LIMIT ? OFFSET ?
            """
            params = (group_id, f"%{search}%", f"%{search}%", f"%{search}%", per_page, offset)
            crawler.db.cursor.execute("SELECT COUNT(*) FROM topics WHERE group_id = ? AND title LIKE ?", (group_id, f"%{search}%"))
        else:
            query = """
                SELECT
                    t.topic_id, t.title, t.create_time, t.likes_count, t.comments_count,
                    t.reading_count, t.type, t.digested, t.sticky,
                    q.text as question_text,
                    a.text as answer_text,
                    tk.text as talk_text,
                    u.user_id, u.name, u.avatar_url, t.imported_at
                FROM topics t
                LEFT JOIN questions q ON t.topic_id = q.topic_id
                LEFT JOIN answers a ON t.topic_id = a.topic_id
                LEFT JOIN talks tk ON t.topic_id = tk.topic_id
                LEFT JOIN users u ON tk.owner_user_id = u.user_id
                WHERE t.group_id = ?
                ORDER BY t.create_time DESC
                LIMIT ? OFFSET ?
            """
            params = (group_id, per_page, offset)
            crawler.db.cursor.execute("SELECT COUNT(*) FROM topics WHERE group_id = ?", (group_id,))

        total = crawler.db.cursor.fetchone()[0]
        crawler.db.cursor.execute(query, params)
        topics = crawler.db.cursor.fetchall()

        topics_list = []
        for topic in topics:
            topic_data = {
                "topic_id": str(topic[0]) if topic[0] is not None else None,
                "title": topic[1],
                "create_time": topic[2],
                "likes_count": topic[3],
                "comments_count": topic[4],
                "reading_count": topic[5],
                "type": topic[6],
                "digested": bool(topic[7]) if topic[7] is not None else False,
                "sticky": bool(topic[8]) if topic[8] is not None else False,
                "imported_at": topic[15] if len(topic) > 15 else None,
            }

            if topic[6] == "q&a":
                topic_data["question_text"] = topic[9] if topic[9] else ""
                topic_data["answer_text"] = topic[10] if topic[10] else ""
            else:
                topic_data["talk_text"] = topic[11] if topic[11] else ""
                if topic[12]:
                    topic_data["author"] = {
                        "user_id": topic[12],
                        "name": topic[13],
                        "avatar_url": topic[14],
                    }

            topics_list.append(topic_data)

        return {
            "topics": topics_list,
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total": total,
                "pages": (total + per_page - 1) // per_page,
            },
        }

    def get_group_stats(self, group_id: int) -> Dict[str, Any]:
        crawler = self._build_crawler(str(group_id))
        cursor = crawler.db.cursor

        cursor.execute("SELECT COUNT(*) FROM topics WHERE group_id = ?", (group_id,))
        topics_count = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT COUNT(DISTINCT t.owner_user_id)
            FROM talks t
            JOIN topics tp ON t.topic_id = tp.topic_id
            WHERE tp.group_id = ?
            """,
            (group_id,),
        )
        users_count = cursor.fetchone()[0]

        cursor.execute("SELECT MAX(create_time) FROM topics WHERE group_id = ?", (group_id,))
        latest_topic_time = cursor.fetchone()[0]

        cursor.execute("SELECT MIN(create_time) FROM topics WHERE group_id = ?", (group_id,))
        earliest_topic_time = cursor.fetchone()[0]

        cursor.execute("SELECT SUM(likes_count) FROM topics WHERE group_id = ?", (group_id,))
        total_likes = cursor.fetchone()[0] or 0

        cursor.execute("SELECT SUM(comments_count) FROM topics WHERE group_id = ?", (group_id,))
        total_comments = cursor.fetchone()[0] or 0

        cursor.execute("SELECT SUM(reading_count) FROM topics WHERE group_id = ?", (group_id,))
        total_readings = cursor.fetchone()[0] or 0

        return {
            "group_id": group_id,
            "topics_count": topics_count,
            "users_count": users_count,
            "latest_topic_time": latest_topic_time,
            "earliest_topic_time": earliest_topic_time,
            "total_likes": total_likes,
            "total_comments": total_comments,
            "total_readings": total_readings,
        }

    def get_group_database_info(self, group_id: int) -> Dict[str, Any]:
        db_info = get_db_path_manager().get_database_info(str(group_id))
        return {"group_id": group_id, "database_info": db_info}

    def delete_group_topics(self, group_id: int) -> Dict[str, Any]:
        crawler = self._build_crawler(str(group_id))
        try:
            crawler.db.cursor.execute("SELECT COUNT(*) FROM topics WHERE group_id = ?", (group_id,))
            topics_count = crawler.db.cursor.fetchone()[0]
            if topics_count == 0:
                return {"message": "该群组没有话题数据", "deleted_count": 0}

            deleted_counts = {}
            for table in self.TOPIC_RELATED_TABLES:
                crawler.db.cursor.execute(
                    f"""
                    DELETE FROM {table}
                    WHERE topic_id IN (
                        SELECT topic_id FROM topics WHERE group_id = ?
                    )
                    """,
                    (group_id,),
                )
                deleted_counts[table] = crawler.db.cursor.rowcount

            crawler.db.cursor.execute("DELETE FROM topics WHERE group_id = ?", (group_id,))
            deleted_counts["topics"] = crawler.db.cursor.rowcount
            crawler.db.conn.commit()
            return {
                "message": f"成功删除群组 {group_id} 的所有话题数据",
                "deleted_topics_count": topics_count,
                "deleted_details": deleted_counts,
            }
        except Exception:
            crawler.db.conn.rollback()
            raise
