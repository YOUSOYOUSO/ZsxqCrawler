from __future__ import annotations

import asyncio
import json
import os
import random
import time
from typing import Any, Dict, Optional

import requests
from fastapi import HTTPException

from modules.accounts.accounts_sql_manager import get_accounts_sql_manager
from modules.shared.db_path_manager import get_db_path_manager
from modules.shared.logger_config import log_debug, log_error, log_exception, log_info
from modules.zsxq.zsxq_columns_database import ZSXQColumnsDatabase
from modules.zsxq.zsxq_interactive_crawler import load_config
from api.schemas.models import ColumnsSettingsRequest
from api.services.task_facade import TaskFacade


class ColumnsService:
    def __init__(self):
        self.tasks = TaskFacade()

    def _get_columns_db(self, group_id: str) -> ZSXQColumnsDatabase:
        path_manager = get_db_path_manager()
        db_path = path_manager.get_columns_db_path(group_id)
        return ZSXQColumnsDatabase(db_path)

    def _resolve_cookie_for_group(self, group_id: str) -> str:
        manager = get_accounts_sql_manager()
        account = manager.get_account_for_group(group_id, mask_cookie=False)
        if account and account.get("cookie"):
            return str(account["cookie"]).strip()

        first = manager.get_first_account(mask_cookie=False)
        if first and first.get("cookie"):
            return str(first["cookie"]).strip()

        cfg = load_config() or {}
        cookie = str((cfg.get("auth", {}) or {}).get("cookie", "")).strip()
        return cookie

    def _build_stealth_headers(self, cookie: str) -> Dict[str, str]:
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        ]
        return {
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Cookie": cookie,
            "Origin": "https://wx.zsxq.com",
            "Pragma": "no-cache",
            "Referer": "https://wx.zsxq.com/",
            "Sec-Ch-Ua": '\"Google Chrome\";v=\"137\", \"Chromium\";v=\"137\", \"Not/A)Brand\";v=\"24\"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '\"Windows\"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "User-Agent": random.choice(user_agents),
            "X-Request-Id": f"dcc5cb6ab-1bc3-8273-cc26-{random.randint(100000000000, 999999999999)}",
            "X-Timestamp": str(int(time.time())),
            "X-Version": "2.77.0",
        }

    def get_group_columns(self, group_id: str) -> Dict[str, Any]:
        db = self._get_columns_db(group_id)
        try:
            columns = db.get_columns(int(group_id))
            stats = db.get_stats(int(group_id))
            return {"columns": columns, "stats": stats}
        finally:
            db.close()

    def get_column_topics(self, group_id: str, column_id: int) -> Dict[str, Any]:
        db = self._get_columns_db(group_id)
        try:
            topics = db.get_column_topics(column_id)
            column = db.get_column(column_id)
            return {"column": column, "topics": topics}
        finally:
            db.close()

    def get_column_topic_detail(self, group_id: str, topic_id: int) -> Dict[str, Any]:
        db = self._get_columns_db(group_id)
        try:
            detail = db.get_topic_detail(topic_id)
        finally:
            db.close()

        if not detail:
            raise HTTPException(status_code=404, detail="文章详情不存在")

        if detail.get("raw_json"):
            try:
                raw_data = json.loads(detail["raw_json"])
                topic_type = raw_data.get("type", "")
                if topic_type == "q&a":
                    question = raw_data.get("question", {})
                    answer = raw_data.get("answer", {})
                    detail["question"] = {
                        "text": question.get("text", ""),
                        "owner": question.get("owner"),
                        "images": question.get("images", []),
                    }
                    detail["answer"] = {
                        "text": answer.get("text", ""),
                        "owner": answer.get("owner"),
                        "images": answer.get("images", []),
                    }
                    if not detail.get("full_text") and answer.get("text"):
                        detail["full_text"] = answer.get("text", "")
                elif topic_type == "talk":
                    talk = raw_data.get("talk", {})
                    if not detail.get("full_text") and talk.get("text"):
                        detail["full_text"] = talk.get("text", "")
            except (json.JSONDecodeError, TypeError):
                pass

        return detail

    def get_group_columns_summary(self, group_id: str) -> Dict[str, Any]:
        cookie = self._resolve_cookie_for_group(group_id)
        if not cookie:
            return {"has_columns": False, "title": None, "error": "未找到可用Cookie"}

        headers = self._build_stealth_headers(cookie)
        url = f"https://api.zsxq.com/v2/groups/{group_id}/columns/summary"
        try:
            response = requests.get(url, headers=headers, timeout=30)
        except requests.RequestException as e:
            return {"has_columns": False, "title": None, "error": f"网络请求失败: {str(e)}"}

        if response.status_code != 200:
            return {"has_columns": False, "title": None, "error": f"HTTP {response.status_code}"}

        data = response.json()
        if not data.get("succeeded"):
            return {"has_columns": False, "title": None, "error": data.get("error_message", "API返回失败")}

        resp_data = data.get("resp_data", {})
        return {"has_columns": resp_data.get("has_columns", False), "title": resp_data.get("title", None)}



    def start_fetch(self, group_id: str, request: Any, background_tasks: Any) -> Dict[str, Any]:
        task_id = self.tasks.create_task("columns_fetch", f"专栏采集 (群组: {group_id})")
        # keep historical prefix for easier frontend filtering
        try:
            now = self.tasks.tasks.get(task_id, {})
            now["task_id"] = task_id
            now["group_id"] = group_id
        except Exception:
            pass
        background_tasks.add_task(self._fetch_columns_task, task_id, group_id, request)
        return {"success": True, "task_id": task_id, "message": "专栏采集任务已启动"}

    async def _fetch_columns_task(self, task_id: str, group_id: str, settings: ColumnsSettingsRequest):
        """专栏采集后台任务"""
        log_id = None
        db = None

        try:
            # 获取配置参数
            crawl_interval_min = settings.crawlIntervalMin or 2.0
            crawl_interval_max = settings.crawlIntervalMax or 5.0
            long_sleep_min = settings.longSleepIntervalMin or 30.0
            long_sleep_max = settings.longSleepIntervalMax or 60.0
            items_per_batch = settings.itemsPerBatch or 10
            download_files = settings.downloadFiles if settings.downloadFiles is not None else True
            download_videos = settings.downloadVideos if settings.downloadVideos is not None else True
            cache_images = settings.cacheImages if settings.cacheImages is not None else True
            incremental_mode = settings.incrementalMode if settings.incrementalMode is not None else False

            self.tasks.append_log(task_id, f"📚 开始采集群组 {group_id} 的专栏内容")
            self.tasks.append_log(task_id, "=" * 50)
            self.tasks.append_log(task_id, "⚙️ 采集配置:")
            self.tasks.append_log(task_id, f"   ⏱️ 请求间隔: {crawl_interval_min}~{crawl_interval_max} 秒")
            self.tasks.append_log(task_id, f"   😴 长休眠间隔: {long_sleep_min}~{long_sleep_max} 秒")
            self.tasks.append_log(task_id, f"   📦 批次大小: {items_per_batch} 个请求")
            self.tasks.append_log(task_id, f"   📥 下载文件: {'是' if download_files else '否'}")
            self.tasks.append_log(task_id, f"   🎬 下载视频: {'是' if download_videos else '否'}")
            self.tasks.append_log(task_id, f"   🖼️ 缓存图片: {'是' if cache_images else '否'}")
            self.tasks.append_log(task_id, f"   🔄 增量模式: {'是（跳过已存在）' if incremental_mode else '否（全量采集）'}")
            self.tasks.append_log(task_id, "=" * 50)

            cookie = self._resolve_cookie_for_group(group_id)
            if not cookie:
                raise Exception("未找到可用Cookie，请先配置账号")

            headers = self._build_stealth_headers(cookie)
            db = self._get_columns_db(group_id)
            log_id = db.start_crawl_log(int(group_id), 'full_fetch')

            columns_count = 0
            topics_count = 0
            details_count = 0
            files_count = 0
            images_count = 0
            videos_count = 0
            skipped_count = 0  # 增量模式跳过的文章数
            files_skipped = 0  # 跳过的文件数（已存在）
            videos_skipped = 0  # 跳过的视频数（已存在）
            request_count = 0  # 请求计数器，用于触发长休眠

            # 1. 获取专栏目录列表（带重试机制）
            self.tasks.append_log(task_id, "📂 获取专栏目录列表...")
            columns_url = f"https://api.zsxq.com/v2/groups/{group_id}/columns"
            max_retries = 10
            columns = None

            for retry in range(max_retries):
                if self.tasks.is_task_stopped(task_id):
                    break

                try:
                    resp = requests.get(columns_url, headers=headers, timeout=30)
                    request_count += 1
                except Exception as req_err:
                    log_exception(f"获取专栏目录请求异常: group_id={group_id}, url={columns_url}")
                    if retry < max_retries - 1:
                        wait_time = 2 if retry < 3 else (5 if retry < 6 else 10)
                        self.tasks.append_log(task_id, f"   ⚠️ 请求异常，等待{wait_time}秒后重试 ({retry+1}/{max_retries})")
                        await asyncio.sleep(wait_time)
                        continue
                    raise Exception(f"获取专栏目录请求异常: {req_err}")

                if resp.status_code != 200:
                    log_error(f"获取专栏目录失败: group_id={group_id}, HTTP {resp.status_code}, response={resp.text[:500] if resp.text else 'empty'}")
                    if retry < max_retries - 1:
                        wait_time = 2 if retry < 3 else (5 if retry < 6 else 10)
                        self.tasks.append_log(task_id, f"   ⚠️ HTTP {resp.status_code}，等待{wait_time}秒后重试 ({retry+1}/{max_retries})")
                        await asyncio.sleep(wait_time)
                        continue
                    raise Exception(f"获取专栏目录失败: HTTP {resp.status_code}")

                try:
                    data = resp.json()
                except Exception as json_err:
                    log_exception(f"解析专栏目录JSON失败: group_id={group_id}, response={resp.text[:500] if resp.text else 'empty'}")
                    raise Exception(f"解析专栏目录失败: {json_err}")

                if not data.get('succeeded'):
                    error_code = data.get('code')
                    error_msg = data.get('error_message', '未知错误')

                    # 检查是否是会员过期
                    if 'expired' in error_msg.lower() or data.get('resp_data', {}).get('expired'):
                        raise Exception(f"会员已过期: {error_msg}")

                    # 检查是否是反爬错误码 1059，需要重试
                    if error_code == 1059:
                        if retry < max_retries - 1:
                            wait_time = 2 if retry < 3 else (5 if retry < 6 else 10)
                            self.tasks.append_log(task_id, f"   ⚠️ 遇到反爬机制 (错误码1059)，等待{wait_time}秒后重试 ({retry+1}/{max_retries})")
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            log_error(f"获取专栏目录重试{max_retries}次后仍失败: group_id={group_id}, code={error_code}")
                            raise Exception(f"获取专栏目录失败，重试{max_retries}次后仍遇到反爬限制")
                    else:
                        log_error(f"获取专栏目录API失败: group_id={group_id}, code={error_code}, message={error_msg}, response={json.dumps(data, ensure_ascii=False)[:500]}")
                        raise Exception(f"API返回失败: {error_msg} (code={error_code})")
                else:
                    # 成功获取
                    columns = data.get('resp_data', {}).get('columns', [])
                    if retry > 0:
                        self.tasks.append_log(task_id, f"   ✅ 重试成功 (第{retry+1}次尝试)")
                    break

            if columns is None:
                raise Exception("获取专栏目录失败")
            self.tasks.append_log(task_id, f"✅ 获取到 {len(columns)} 个专栏目录")

            if len(columns) == 0:
                self.tasks.append_log(task_id, "ℹ️ 该群组没有专栏内容")
                self.tasks.update_task(task_id, "completed", "该群组没有专栏内容")
                db.close()
                return

            # 2. 遍历每个专栏
            for col_idx, column in enumerate(columns, 1):
                if self.tasks.is_task_stopped(task_id):
                    self.tasks.append_log(task_id, "🛑 任务已被用户停止")
                    break

                column_id = column.get('column_id')
                column_name = column.get('name', '未命名')
                column_topics_count = column.get('statistics', {}).get('topics_count', 0)
                db.insert_column(int(group_id), column)
                columns_count += 1

                self.tasks.append_log(task_id, "")
                self.tasks.append_log(task_id, f"📁 [{col_idx}/{len(columns)}] 专栏: {column_name}")
                self.tasks.append_log(task_id, f"   📊 预计文章数: {column_topics_count}")

                # 检查是否需要长休眠
                if request_count > 0 and request_count % items_per_batch == 0:
                    sleep_time = random.uniform(long_sleep_min, long_sleep_max)
                    self.tasks.append_log(task_id, f"   😴 已完成 {request_count} 次请求，休眠 {sleep_time:.1f} 秒...")
                    await asyncio.sleep(sleep_time)

                # 随机延迟
                delay = random.uniform(crawl_interval_min, crawl_interval_max)
                self.tasks.append_log(task_id, f"   ⏳ 等待 {delay:.1f} 秒后获取文章列表...")
                await asyncio.sleep(delay)

                # 获取专栏文章列表
                topics_url = f"https://api.zsxq.com/v2/groups/{group_id}/columns/{column_id}/topics?count=100&sort=default&direction=desc"
                try:
                    topics_resp = requests.get(topics_url, headers=headers, timeout=30)
                    request_count += 1
                except Exception as req_err:
                    log_exception(f"获取专栏文章列表请求异常: column_id={column_id}, url={topics_url}")
                    self.tasks.append_log(task_id, f"   ⚠️ 获取文章列表请求异常: {req_err}")
                    continue

                if topics_resp.status_code != 200:
                    log_error(f"获取专栏文章列表失败: column_id={column_id}, HTTP {topics_resp.status_code}, response={topics_resp.text[:500] if topics_resp.text else 'empty'}")
                    self.tasks.append_log(task_id, f"   ⚠️ 获取文章列表失败: HTTP {topics_resp.status_code}")
                    continue

                try:
                    topics_data = topics_resp.json()
                except Exception as json_err:
                    log_exception(f"解析专栏文章列表JSON失败: column_id={column_id}, response={topics_resp.text[:500] if topics_resp.text else 'empty'}")
                    self.tasks.append_log(task_id, f"   ⚠️ 解析文章列表失败: {json_err}")
                    continue

                if not topics_data.get('succeeded'):
                    error_code = topics_data.get('code', 'unknown')
                    error_message = topics_data.get('error_message', '未知错误')
                    log_error(f"获取专栏文章列表失败: column_id={column_id}, code={error_code}, message={error_message}")
                    self.tasks.append_log(task_id, f"   ⚠️ 获取文章列表失败: {error_message} (code={error_code})")
                    continue

                topics_list = topics_data.get('resp_data', {}).get('topics', [])
                self.tasks.append_log(task_id, f"   📝 获取到 {len(topics_list)} 篇文章")

                # 3. 遍历每篇文章
                for topic_idx, topic in enumerate(topics_list, 1):
                    if self.tasks.is_task_stopped(task_id):
                        break

                    topic_id = topic.get('topic_id')
                    topic_title = topic.get('title', '无标题')[:30]
                    db.insert_column_topic(column_id, int(group_id), topic)
                    topics_count += 1

                    # 增量模式：检查文章详情是否已存在
                    if incremental_mode and db.topic_detail_exists(topic_id):
                        self.tasks.append_log(task_id, f"   📄 [{topic_idx}/{len(topics_list)}] {topic_title}... ⏭️ 跳过（已存在）")
                        skipped_count += 1
                        continue

                    self.tasks.append_log(task_id, f"   📄 [{topic_idx}/{len(topics_list)}] {topic_title}...")

                    # 获取文章详情（带重试机制）
                    max_retries = 10
                    topic_detail = None

                    for retry in range(max_retries):
                        if self.tasks.is_task_stopped(task_id):
                            break

                        # 检查是否需要长休眠
                        if request_count > 0 and request_count % items_per_batch == 0:
                            sleep_time = random.uniform(long_sleep_min, long_sleep_max)
                            self.tasks.append_log(task_id, f"      😴 已完成 {request_count} 次请求，休眠 {sleep_time:.1f} 秒...")
                            await asyncio.sleep(sleep_time)

                        # 随机延迟
                        delay = random.uniform(crawl_interval_min, crawl_interval_max)
                        await asyncio.sleep(delay)

                        # 获取文章详情
                        detail_url = f"https://api.zsxq.com/v2/topics/{topic_id}/info"
                        try:
                            detail_resp = requests.get(detail_url, headers=headers, timeout=30)
                            request_count += 1
                        except Exception as req_err:
                            log_exception(f"获取文章详情请求异常: topic_id={topic_id}, url={detail_url}")
                            self.tasks.append_log(task_id, f"      ⚠️ 获取详情请求异常: {req_err}")
                            continue

                        if detail_resp.status_code != 200:
                            log_error(f"获取文章详情失败: topic_id={topic_id}, HTTP {detail_resp.status_code}, response={detail_resp.text[:500] if detail_resp.text else 'empty'}")
                            self.tasks.append_log(task_id, f"      ⚠️ 获取详情失败: HTTP {detail_resp.status_code}")
                            continue

                        try:
                            detail_data = detail_resp.json()
                        except Exception as json_err:
                            log_exception(f"解析文章详情JSON失败: topic_id={topic_id}, response={detail_resp.text[:500] if detail_resp.text else 'empty'}")
                            self.tasks.append_log(task_id, f"      ⚠️ 解析详情失败: {json_err}")
                            continue

                        if not detail_data.get('succeeded'):
                            error_code = detail_data.get('code')
                            error_message = detail_data.get('error_message', '未知错误')

                            # 检查是否是反爬错误码 1059，需要重试
                            if error_code == 1059:
                                if retry < max_retries - 1:
                                    # 智能等待时间策略：前几次短等待，后面逐渐增加
                                    if retry < 3:
                                        wait_time = 2  # 前3次等待2秒
                                    elif retry < 6:
                                        wait_time = 5  # 第4-6次等待5秒
                                    else:
                                        wait_time = 10  # 第7-10次等待10秒

                                    self.tasks.append_log(task_id, f"      ⚠️ 遇到反爬机制 (错误码1059)，等待{wait_time}秒后重试 ({retry+1}/{max_retries})")
                                    await asyncio.sleep(wait_time)
                                    continue
                                else:
                                    log_error(f"获取文章详情重试{max_retries}次后仍失败: topic_id={topic_id}, code={error_code}, message={error_message}")
                                    self.tasks.append_log(task_id, f"      ❌ 重试{max_retries}次后仍失败: {error_message} (code={error_code})")
                                    break
                            else:
                                log_error(f"获取文章详情失败: topic_id={topic_id}, code={error_code}, message={error_message}, full_response={json.dumps(detail_data, ensure_ascii=False)[:500]}")
                                self.tasks.append_log(task_id, f"      ⚠️ 获取详情失败: {error_message} (code={error_code})")
                                break
                        else:
                            # 成功获取详情
                            topic_detail = detail_data.get('resp_data', {}).get('topic', {})
                            if retry > 0:
                                self.tasks.append_log(task_id, f"      ✅ 重试成功 (第{retry+1}次尝试)")
                            break

                    # 如果没有获取到详情，跳过后续处理
                    if not topic_detail:
                        continue
                    db.insert_topic_detail(int(group_id), topic_detail, json.dumps(topic_detail, ensure_ascii=False))
                    details_count += 1

                    # 处理文件下载
                    if download_files:
                        talk = topic_detail.get('talk', {})
                        topic_files = talk.get('files', [])
                        content_voice = topic_detail.get('content_voice')

                        all_files = topic_files.copy()
                        if content_voice:
                            all_files.append(content_voice)

                        for file_info in all_files:
                            if self.tasks.is_task_stopped(task_id):
                                break

                            file_id = file_info.get('file_id')
                            file_name = file_info.get('name', '')
                            file_size = file_info.get('size', 0)

                            if file_id:
                                self.tasks.append_log(task_id, f"      📥 下载文件: {file_name[:40]}...")

                                # 检查是否需要长休眠
                                if request_count > 0 and request_count % items_per_batch == 0:
                                    sleep_time = random.uniform(long_sleep_min, long_sleep_max)
                                    self.tasks.append_log(task_id, f"      😴 已完成 {request_count} 次请求，休眠 {sleep_time:.1f} 秒...")
                                    await asyncio.sleep(sleep_time)

                                delay = random.uniform(crawl_interval_min, crawl_interval_max)
                                await asyncio.sleep(delay)

                                try:
                                    result = await self._download_column_file(
                                        group_id, file_id, file_name, file_size,
                                        topic_id, db, headers, task_id
                                    )
                                    if result == "downloaded":
                                        files_count += 1
                                        request_count += 1
                                        self.tasks.append_log(task_id, f"         ✅ 文件下载成功")
                                    elif result == "skipped":
                                        files_skipped += 1
                                    # "skipped" 时日志已在函数内输出
                                except Exception as fe:
                                    log_exception(f"文件下载失败: file_id={file_id}, file_name={file_name}, topic_id={topic_id}")
                                    self.tasks.append_log(task_id, f"         ⚠️ 文件下载失败: {fe}")

                    # 缓存图片
                    if cache_images:
                        talk = topic_detail.get('talk', {}) if 'talk' in topic_detail else {}
                        topic_images = talk.get('images', [])

                        for image in topic_images:
                            if self.tasks.is_task_stopped(task_id):
                                break

                            original_url = image.get('original', {}).get('url')
                            image_id = image.get('image_id')

                            if original_url and image_id:
                                try:
                                    cache_manager = get_image_cache_manager(group_id)
                                    success, local_path, error_msg = cache_manager.download_and_cache(original_url)
                                    if success and local_path:
                                        db.update_image_local_path(image_id, str(local_path))
                                        images_count += 1
                                    elif error_msg:
                                        self.tasks.append_log(task_id, f"      ⚠️ 图片缓存失败: {error_msg}")
                                except Exception as ie:
                                    log_exception(f"图片缓存失败: image_id={image_id}, url={original_url}")
                                    self.tasks.append_log(task_id, f"      ⚠️ 图片缓存失败: {ie}")

                    # 处理视频
                    talk_for_video = topic_detail.get('talk', {}) if 'talk' in topic_detail else {}
                    video = talk_for_video.get('video')

                    if video and video.get('video_id'):
                        video_id = video.get('video_id')
                        video_size = video.get('size', 0)
                        video_duration = video.get('duration', 0)
                        cover = video.get('cover', {})
                        cover_url = cover.get('url')

                        self.tasks.append_log(task_id, f"      🎬 发现视频: ID={video_id}, 大小={video_size/(1024*1024):.1f}MB, 时长={video_duration}秒")

                        # 缓存视频封面（跟随图片缓存选项）
                        if cache_images and cover_url:
                            try:
                                cache_manager = get_image_cache_manager(group_id)
                                success, cover_local, error_msg = cache_manager.download_and_cache(cover_url)
                                if success and cover_local:
                                    db.update_video_cover_path(video_id, str(cover_local))
                                    self.tasks.append_log(task_id, f"      ✅ 视频封面缓存成功")
                                elif error_msg:
                                    log_warning(f"视频封面缓存失败: video_id={video_id}, url={cover_url}, error={error_msg}")
                                    self.tasks.append_log(task_id, f"      ⚠️ 视频封面缓存失败: {error_msg}")
                            except Exception as ve:
                                log_exception(f"视频封面缓存失败: video_id={video_id}, url={cover_url}")
                                self.tasks.append_log(task_id, f"      ⚠️ 视频封面缓存失败: {ve}")

                        # 下载视频（单独控制）
                        if download_videos:
                            if request_count > 0 and request_count % items_per_batch == 0:
                                sleep_time = random.uniform(long_sleep_min, long_sleep_max)
                                self.tasks.append_log(task_id, f"      😴 已完成 {request_count} 次请求，休眠 {sleep_time:.1f} 秒...")
                                await asyncio.sleep(sleep_time)

                            delay = random.uniform(crawl_interval_min, crawl_interval_max)
                            await asyncio.sleep(delay)

                            try:
                                result = await self._download_column_video(
                                    group_id, video_id, video_size, video_duration,
                                    topic_id, db, headers, task_id
                                )
                                if result == "downloaded":
                                    videos_count += 1
                                    request_count += 1
                                elif result == "skipped":
                                    videos_skipped += 1
                                # "skipped" 时日志已在函数内输出
                            except Exception as ve:
                                log_exception(f"视频下载失败: video_id={video_id}, topic_id={topic_id}, size={video_size}")
                                self.tasks.append_log(task_id, f"      ⚠️ 视频下载失败: {ve}")
                        else:
                            self.tasks.append_log(task_id, f"      ⏭️ 跳过视频下载（已禁用）")

                    # 更新进度
                    self.tasks.update_task(task_id, "running", f"进度: {details_count} 篇文章, {files_count} 个文件, {videos_count} 个视频, {images_count} 张图片")

            # 完成
            self.tasks.append_log(task_id, "")
            self.tasks.append_log(task_id, "=" * 50)
            self.tasks.append_log(task_id, "🎉 专栏采集完成！")
            self.tasks.append_log(task_id, f"📊 统计:")
            self.tasks.append_log(task_id, f"   📁 专栏目录: {columns_count} 个")
            self.tasks.append_log(task_id, f"   📝 文章列表: {topics_count} 篇")
            self.tasks.append_log(task_id, f"   📄 文章详情: {details_count} 篇（新增）")
            if skipped_count > 0:
                self.tasks.append_log(task_id, f"   ⏭️ 跳过已存在文章: {skipped_count} 篇")
            self.tasks.append_log(task_id, f"   📥 下载文件: {files_count} 个" + (f" (跳过 {files_skipped} 个已存在)" if files_skipped > 0 else ""))
            self.tasks.append_log(task_id, f"   🎬 下载视频: {videos_count} 个" + (f" (跳过 {videos_skipped} 个已存在)" if videos_skipped > 0 else ""))
            self.tasks.append_log(task_id, f"   🖼️ 缓存图片: {images_count} 张")
            self.tasks.append_log(task_id, f"   📡 总请求数: {request_count} 次")
            self.tasks.append_log(task_id, "=" * 50)

            db.update_crawl_log(log_id, columns_count=columns_count, topics_count=topics_count,
                              details_count=details_count, files_count=files_count, status='completed')
            db.close()

            skipped_info = f", 跳过 {skipped_count} 篇" if skipped_count > 0 else ""
            result_msg = f"采集完成: {columns_count} 个专栏, {details_count} 篇新文章{skipped_info}, {files_count} 个文件, {videos_count} 个视频"
            self.tasks.update_task(task_id, "completed", result_msg)

        except Exception as e:
            error_msg = str(e)
            self.tasks.append_log(task_id, "")
            self.tasks.append_log(task_id, f"❌ 采集失败: {error_msg}")
            self.tasks.update_task(task_id, "failed", f"采集失败: {error_msg}")

            try:
                if db and log_id:
                    db.update_crawl_log(log_id, status='failed', error_message=error_msg)
                    db.close()
            except:
                pass


    async def _download_column_file(self, group_id: str, file_id: int, file_name: str, file_size: int,
                                    topic_id: int, db: ZSXQColumnsDatabase, headers: dict, task_id: str = None) -> str:
        """下载专栏文件

        Returns:
            str: "downloaded" 表示新下载, "skipped" 表示已存在跳过, 或抛出异常
        """
        # 先检查本地文件是否已存在
        path_manager = get_db_path_manager()
        group_dir = path_manager.get_group_dir(group_id)
        downloads_dir = os.path.join(group_dir, "column_downloads")
        local_path = os.path.join(downloads_dir, file_name)

        # 如果文件已存在且大小匹配，跳过下载
        if os.path.exists(local_path):
            existing_size = os.path.getsize(local_path)
            if existing_size == file_size or (file_size == 0 and existing_size > 0):
                db.update_file_download_status(file_id, 'completed', local_path)
                if task_id:
                    self.tasks.append_log(task_id, f"         ⏭️ 文件已存在，跳过下载 ({existing_size/(1024*1024):.2f}MB)")
                return "skipped"

        # 获取下载URL（带重试机制）
        download_url = f"https://api.zsxq.com/v2/files/{file_id}/download_url"
        max_retries = 10
        real_url = None

        for retry in range(max_retries):
            try:
                resp = requests.get(download_url, headers=headers, timeout=30)
            except Exception as req_err:
                if retry < max_retries - 1:
                    wait_time = 2 if retry < 3 else (5 if retry < 6 else 10)
                    await asyncio.sleep(wait_time)
                    continue
                log_exception(f"获取下载链接请求异常: file_id={file_id}")
                raise Exception(f"获取下载链接请求异常: {req_err}")

            if resp.status_code != 200:
                if retry < max_retries - 1:
                    wait_time = 2 if retry < 3 else (5 if retry < 6 else 10)
                    await asyncio.sleep(wait_time)
                    continue
                error_msg = f"获取下载链接失败: HTTP {resp.status_code}, URL={download_url}, Response={resp.text[:500] if resp.text else 'empty'}"
                log_error(error_msg)
                raise Exception(error_msg)

            data = resp.json()
            if not data.get('succeeded'):
                error_code = data.get('code')
                error_message = data.get('error_message', '未知错误')

                # 检查是否是反爬错误码 1059，需要重试
                if error_code == 1059:
                    if retry < max_retries - 1:
                        wait_time = 2 if retry < 3 else (5 if retry < 6 else 10)
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        log_error(f"获取下载链接重试{max_retries}次后仍失败: file_id={file_id}, code={error_code}")
                        raise Exception(f"获取下载链接失败，重试{max_retries}次后仍遇到反爬限制")
                else:
                    error_msg = f"获取下载链接失败: code={error_code}, message={error_message}, file_id={file_id}, file_name={file_name}"
                    log_error(error_msg)
                    raise Exception(f"获取下载链接失败: {error_message} (code={error_code})")
            else:
                real_url = data.get('resp_data', {}).get('download_url')
                break

        if not real_url:
            raise Exception("下载链接为空")

        # 创建下载目录（downloads_dir 和 local_path 在函数开头已定义）
        os.makedirs(downloads_dir, exist_ok=True)

        # 下载文件（带重试机制，处理 SSL 错误等网络问题）
        download_retries = 3
        last_error = None

        for download_attempt in range(download_retries):
            try:
                file_resp = requests.get(real_url, headers=headers, stream=True, timeout=300)
                if file_resp.status_code == 200:
                    with open(local_path, 'wb') as f:
                        for chunk in file_resp.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)

                    db.update_file_download_status(file_id, 'completed', local_path)
                    return "downloaded"
                else:
                    last_error = f"HTTP {file_resp.status_code}"
                    if download_attempt < download_retries - 1:
                        log_warning(f"文件下载失败 (尝试 {download_attempt + 1}/{download_retries}): {last_error}, file_id={file_id}")
                        await asyncio.sleep(2 * (download_attempt + 1))  # 递增等待
                        continue
            except requests.exceptions.SSLError as ssl_err:
                last_error = f"SSL错误: {ssl_err}"
                if download_attempt < download_retries - 1:
                    log_warning(f"文件下载SSL错误 (尝试 {download_attempt + 1}/{download_retries}): file_id={file_id}, error={ssl_err}")
                    await asyncio.sleep(3 * (download_attempt + 1))  # SSL错误等待更久
                    continue
            except requests.exceptions.RequestException as req_err:
                last_error = f"网络错误: {req_err}"
                if download_attempt < download_retries - 1:
                    log_warning(f"文件下载网络错误 (尝试 {download_attempt + 1}/{download_retries}): file_id={file_id}, error={req_err}")
                    await asyncio.sleep(2 * (download_attempt + 1))
                    continue

        # 所有重试都失败
        db.update_file_download_status(file_id, 'failed')
        raise Exception(f"下载失败 (重试{download_retries}次): {last_error}")


    async def _download_column_video(self, group_id: str, video_id: int, video_size: int, video_duration: int,
                                     topic_id: int, db: ZSXQColumnsDatabase, headers: dict, task_id: str = None) -> str:
        """下载专栏视频（m3u8格式）

        Returns:
            str: "downloaded" 表示新下载, "skipped" 表示已存在跳过, 或抛出异常
        """
        import subprocess
        import re

        # 先检查本地视频是否已存在
        path_manager = get_db_path_manager()
        group_dir = path_manager.get_group_dir(group_id)
        videos_dir = os.path.join(group_dir, "column_videos")
        video_filename = f"video_{video_id}.mp4"
        local_path = os.path.join(videos_dir, video_filename)

        # 如果视频已存在且大小>0，跳过下载
        if os.path.exists(local_path):
            existing_size = os.path.getsize(local_path)
            if existing_size > 0:
                db.update_video_download_status(video_id, 'completed', '', local_path)
                if task_id:
                    self.tasks.append_log(task_id, f"         ⏭️ 视频已存在，跳过下载 ({existing_size/(1024*1024):.1f}MB)")
                return "skipped"

        # 获取视频URL（带重试机制）
        video_url_api = f"https://api.zsxq.com/v2/videos/{video_id}/url"
        max_retries = 10
        m3u8_url = None

        for retry in range(max_retries):
            try:
                resp = requests.get(video_url_api, headers=headers, timeout=30)
            except Exception as req_err:
                if retry < max_retries - 1:
                    wait_time = 2 if retry < 3 else (5 if retry < 6 else 10)
                    await asyncio.sleep(wait_time)
                    continue
                log_exception(f"获取视频链接请求异常: video_id={video_id}")
                raise Exception(f"获取视频链接请求异常: {req_err}")

            if resp.status_code != 200:
                if retry < max_retries - 1:
                    wait_time = 2 if retry < 3 else (5 if retry < 6 else 10)
                    await asyncio.sleep(wait_time)
                    continue
                error_msg = f"获取视频链接失败: HTTP {resp.status_code}, URL={video_url_api}, Response={resp.text[:500] if resp.text else 'empty'}"
                log_error(error_msg)
                raise Exception(error_msg)

            data = resp.json()
            if not data.get('succeeded'):
                error_code = data.get('code')
                error_message = data.get('error_message', '未知错误')

                # 检查是否是反爬错误码 1059，需要重试
                if error_code == 1059:
                    if retry < max_retries - 1:
                        wait_time = 2 if retry < 3 else (5 if retry < 6 else 10)
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        log_error(f"获取视频链接重试{max_retries}次后仍失败: video_id={video_id}, code={error_code}")
                        raise Exception(f"获取视频链接失败，重试{max_retries}次后仍遇到反爬限制")
                else:
                    error_msg = f"获取视频链接失败: code={error_code}, message={error_message}, video_id={video_id}, topic_id={topic_id}"
                    log_error(error_msg)
                    raise Exception(f"获取视频链接失败: {error_message} (code={error_code})")
            else:
                m3u8_url = data.get('resp_data', {}).get('url')
                break

        if not m3u8_url:
            raise Exception("视频链接为空")

        # 创建视频下载目录（videos_dir 和 local_path 在函数开头已定义）
        os.makedirs(videos_dir, exist_ok=True)

        # 更新状态为下载中
        db.update_video_download_status(video_id, 'downloading', m3u8_url)

        # 使用ffmpeg下载m3u8视频
        try:
            # 检查ffmpeg是否可用
            ffmpeg_check = subprocess.run(['ffmpeg', '-version'], capture_output=True, text=True)
            if ffmpeg_check.returncode != 0:
                raise Exception("ffmpeg not available")

            # 构建 HTTP headers 字符串给 ffmpeg
            # ffmpeg 需要的格式是 "Header1: Value1\r\nHeader2: Value2\r\n"
            ffmpeg_headers = ""
            if headers.get('Cookie'):
                ffmpeg_headers += f"Cookie: {headers['Cookie']}\r\n"
            if headers.get('cookie'):
                ffmpeg_headers += f"Cookie: {headers['cookie']}\r\n"
            ffmpeg_headers += "Referer: https://wx.zsxq.com/\r\n"
            ffmpeg_headers += "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36\r\n"
            ffmpeg_headers += "Origin: https://wx.zsxq.com\r\n"

            # 使用ffmpeg下载（带请求头和进度显示）
            cmd = [
                'ffmpeg', '-y',
                '-headers', ffmpeg_headers,
                '-i', m3u8_url,
                '-c', 'copy',
                '-bsf:a', 'aac_adtstoasc',
                '-progress', 'pipe:1',  # 输出进度信息到 stdout
                local_path
            ]

            log_info(f"开始下载视频: video_id={video_id}, url={m3u8_url[:100]}...")
            if task_id:
                self.tasks.append_log(task_id, f"         🎬 开始下载视频 (预计时长: {video_duration}秒, 大小: {video_size/(1024*1024):.1f}MB)")

            # 使用 Popen 实时读取进度
            # 在 Windows 上需要特殊处理管道缓冲
            import threading
            import queue

            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1
            )

            stderr_output = []
            stdout_queue = queue.Queue()

            # 使用线程读取 stdout，避免阻塞
            def read_stdout():
                try:
                    for line in iter(process.stdout.readline, ''):
                        if line:
                            stdout_queue.put(line)
                        if process.poll() is not None:
                            break
                except:
                    pass

            # 使用线程读取 stderr
            def read_stderr():
                try:
                    for line in iter(process.stderr.readline, ''):
                        if line:
                            stderr_output.append(line)
                except:
                    pass

            stdout_thread = threading.Thread(target=read_stdout, daemon=True)
            stderr_thread = threading.Thread(target=read_stderr, daemon=True)
            stdout_thread.start()
            stderr_thread.start()

            last_log_time = time.time()
            start_time = time.time()

            # 读取进度信息
            try:
                while process.poll() is None:
                    # 非阻塞方式获取进度
                    try:
                        line = stdout_queue.get(timeout=1)

                        # 解析 ffmpeg 进度信息
                        # 格式: out_time_ms=123456789
                        if line.startswith('out_time_ms='):
                            try:
                                time_ms = int(line.split('=')[1].strip())
                                current_seconds = time_ms / 1000000

                                # 每 3 秒更新一次日志，避免刷屏
                                now = time.time()
                                if task_id and (now - last_log_time) >= 3:
                                    if video_duration > 0:
                                        progress_pct = min(100, (current_seconds / video_duration) * 100)
                                        # 生成进度条
                                        bar_length = 20
                                        filled = int(bar_length * progress_pct / 100)
                                        bar = '█' * filled + '░' * (bar_length - filled)
                                        self.tasks.append_log(task_id, f"         📊 下载进度: [{bar}] {progress_pct:.1f}% ({current_seconds:.0f}s/{video_duration}s)")
                                    else:
                                        self.tasks.append_log(task_id, f"         📊 下载进度: {current_seconds:.0f}秒")
                                    last_log_time = now
                            except:
                                pass
                    except queue.Empty:
                        # 队列为空，检查是否需要显示等待中的进度
                        now = time.time()
                        elapsed = now - start_time
                        if task_id and (now - last_log_time) >= 5:
                            self.tasks.append_log(task_id, f"         ⏳ 下载中... (已用时 {elapsed:.0f}秒)")
                            last_log_time = now
                        continue

                # 等待线程结束
                stdout_thread.join(timeout=5)
                stderr_thread.join(timeout=5)

            except Exception as e:
                process.kill()
                raise Exception(f"视频下载异常: {e}")

            returncode = process.returncode
            stderr_text = ''.join(stderr_output)

            # 检查文件是否成功下载（ffmpeg 可能返回非 0 但文件已成功下载）
            if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
                db.update_video_download_status(video_id, 'completed', m3u8_url, local_path)
                final_size = os.path.getsize(local_path)
                log_info(f"视频下载成功: video_id={video_id}, path={local_path}, size={final_size}")
                if task_id:
                    self.tasks.append_log(task_id, f"         ✅ 视频下载完成 ({final_size/(1024*1024):.1f}MB)")
                return "downloaded"
            else:
                db.update_video_download_status(video_id, 'failed', m3u8_url)
                # 从 stderr 中提取真正的错误信息（跳过版本信息等）
                stderr_lines = stderr_text.strip().split('\n')
                # 查找包含 "error" 或 "failed" 的行
                error_lines = [line for line in stderr_lines if 'error' in line.lower() or 'failed' in line.lower() or 'invalid' in line.lower()]
                if error_lines:
                    error_msg = '; '.join(error_lines[-3:])  # 取最后 3 条错误
                else:
                    # 如果没找到明确错误，取最后几行
                    error_msg = '; '.join(stderr_lines[-3:]) if stderr_lines else 'unknown error'
                log_error(f"ffmpeg下载失败: video_id={video_id}, returncode={returncode}, error={error_msg}")
                raise Exception(f"ffmpeg下载失败: {error_msg[:300]}")

        except FileNotFoundError:
            # ffmpeg不可用，保存m3u8链接供手动下载
            db.update_video_download_status(video_id, 'pending_manual', m3u8_url)
            # 保存m3u8链接到文件
            m3u8_link_file = os.path.join(videos_dir, f"video_{video_id}.m3u8.txt")
            with open(m3u8_link_file, 'w', encoding='utf-8') as f:
                f.write(f"Video ID: {video_id}\n")
                f.write(f"Duration: {video_duration} seconds\n")
                f.write(f"Size: {video_size} bytes\n")
                f.write(f"M3U8 URL: {m3u8_url}\n")
            raise Exception("ffmpeg未安装，已保存m3u8链接到文件，请手动下载")
        except subprocess.TimeoutExpired:
            db.update_video_download_status(video_id, 'failed', m3u8_url)
            raise Exception("视频下载超时")


    # migrated to api/routers/columns.py: 

    def get_columns_stats(self, group_id: str) -> Dict[str, Any]:
        db = self._get_columns_db(group_id)
        try:
            return db.get_stats(int(group_id))
        finally:
            db.close()

    def delete_all_columns(self, group_id: str) -> Dict[str, Any]:
        db = self._get_columns_db(group_id)
        try:
            stats = db.clear_all_data(int(group_id))
            return {"success": True, "message": "已清空专栏数据", "deleted": stats}
        finally:
            db.close()

    def get_column_topic_full_comments(self, group_id: str, topic_id: int) -> Dict[str, Any]:
        cookie = self._resolve_cookie_for_group(group_id)
        if not cookie:
            raise HTTPException(status_code=400, detail="No valid account found for this group")

        headers = self._build_stealth_headers(cookie)
        comments_url = f"https://api.zsxq.com/v2/topics/{topic_id}/comments?sort=asc&count=30&with_sticky=true"
        log_info(f"Fetching comments from: {comments_url}")

        resp = requests.get(comments_url, headers=headers, timeout=30)
        if resp.status_code != 200:
            log_error(f"Failed to fetch comments: HTTP {resp.status_code}, response={resp.text[:500] if resp.text else 'empty'}")
            raise HTTPException(status_code=resp.status_code, detail=f"Failed to fetch comments: HTTP {resp.status_code}")

        data = resp.json()
        log_debug(
            f"Comments API response: succeeded={data.get('succeeded')}, resp_data keys={list(data.get('resp_data', {}).keys()) if data.get('resp_data') else 'None'}"
        )
        if not data.get("succeeded"):
            resp_data = data.get("resp_data", {})
            error_msg = resp_data.get("message") or resp_data.get("error_msg") or data.get("error_msg") or data.get("message")
            error_code = resp_data.get("code") or resp_data.get("error_code") or data.get("code")
            log_error(f"Comments API failed: code={error_code}, message={error_msg}, full_response={json.dumps(data, ensure_ascii=False)[:500]}")
            raise HTTPException(status_code=400, detail=f"API error: {error_msg or 'Request failed'} (code: {error_code})")

        comments = data.get("resp_data", {}).get("comments", [])
        processed_comments = []
        for comment in comments:
            processed = {
                "comment_id": comment.get("comment_id"),
                "parent_comment_id": comment.get("parent_comment_id"),
                "text": comment.get("text", ""),
                "create_time": comment.get("create_time"),
                "likes_count": comment.get("likes_count", 0),
                "rewards_count": comment.get("rewards_count", 0),
                "replies_count": comment.get("replies_count", 0),
                "sticky": comment.get("sticky", False),
                "owner": comment.get("owner"),
                "repliee": comment.get("repliee"),
                "images": comment.get("images", []),
            }
            replied_comments = comment.get("replied_comments", [])
            if replied_comments:
                processed["replied_comments"] = [
                    {
                        "comment_id": rc.get("comment_id"),
                        "parent_comment_id": rc.get("parent_comment_id"),
                        "text": rc.get("text", ""),
                        "create_time": rc.get("create_time"),
                        "likes_count": rc.get("likes_count", 0),
                        "owner": rc.get("owner"),
                        "repliee": rc.get("repliee"),
                        "images": rc.get("images", []),
                    }
                    for rc in replied_comments
                ]
            processed_comments.append(processed)

        try:
            db = self._get_columns_db(group_id)
            saved_count = db.import_comments(topic_id, processed_comments)
            db.close()
            log_info(f"Saved {saved_count} comments to database for topic {topic_id}")
        except Exception as e:
            log_error(f"Failed to save comments to database: {e}")

        total_count = sum(1 + len(c.get("replied_comments", [])) for c in processed_comments)
        return {"success": True, "comments": processed_comments, "total": total_count}
