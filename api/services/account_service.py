from __future__ import annotations

from typing import Any, Dict

import requests
from fastapi import HTTPException

from api.schemas.models import AccountCreateRequest, AssignGroupAccountRequest
from api.services.account_resolution_service import (
    clear_account_detect_cache,
    get_account_summary_for_group_auto,
    get_cookie_for_group,
)
from modules.accounts.account_info_db import get_account_info_db
from modules.accounts.accounts_sql_manager import get_accounts_sql_manager


class AccountService:
    def _build_stealth_headers(self, cookie: str) -> Dict[str, str]:
        return {
            "Cookie": cookie,
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Referer": "https://wx.zsxq.com/",
            "Origin": "https://wx.zsxq.com",
            "Accept": "application/json, text/plain, */*",
        }

    def list_accounts(self) -> Dict[str, Any]:
        sql_mgr = get_accounts_sql_manager()
        accounts = sql_mgr.get_accounts(mask_cookie=True)
        return {"accounts": accounts}

    def create_account(self, request: AccountCreateRequest) -> Dict[str, Any]:
        sql_mgr = get_accounts_sql_manager()
        acc = sql_mgr.add_account(request.cookie, request.name)
        safe_acc = sql_mgr.get_account_by_id(acc.get("id"), mask_cookie=True)
        clear_account_detect_cache()
        return {"account": safe_acc}

    def remove_account(self, account_id: str) -> Dict[str, Any]:
        sql_mgr = get_accounts_sql_manager()
        ok = sql_mgr.delete_account(account_id)
        if not ok:
            raise HTTPException(status_code=404, detail="Account does not exist")
        clear_account_detect_cache()
        return {"success": True}

    def assign_account_to_group(self, group_id: str, request: AssignGroupAccountRequest) -> Dict[str, Any]:
        sql_mgr = get_accounts_sql_manager()
        ok, msg = sql_mgr.assign_group_account(group_id, request.account_id)
        if not ok:
            raise HTTPException(status_code=400, detail=msg)
        return {"success": True, "message": msg}

    def get_group_account(self, group_id: str) -> Dict[str, Any]:
        summary = get_account_summary_for_group_auto(group_id)
        return {"account": summary}

    def _extract_self_info(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        rd = payload.get("resp_data", {}) or {}
        user = rd.get("user", {}) or {}
        wechat = (rd.get("accounts", {}) or {}).get("wechat", {}) or {}
        return {
            "uid": user.get("uid"),
            "name": user.get("name") or wechat.get("name"),
            "avatar_url": user.get("avatar_url") or wechat.get("avatar_url"),
            "location": user.get("location"),
            "user_sid": user.get("user_sid"),
            "grade": user.get("grade"),
        }

    def _fetch_self_payload(self, cookie: str) -> Dict[str, Any]:
        headers = self._build_stealth_headers(cookie)
        resp = requests.get("https://api.zsxq.com/v3/users/self", headers=headers, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        if not payload.get("succeeded"):
            raise HTTPException(status_code=400, detail="API returned failure")
        return payload

    def get_account_self(self, account_id: str) -> Dict[str, Any]:
        db = get_account_info_db()
        info = db.get_self_info(account_id)
        if info:
            return {"self": info}

        sql_mgr = get_accounts_sql_manager()
        acc = sql_mgr.get_account_by_id(account_id, mask_cookie=False)
        if not acc:
            raise HTTPException(status_code=404, detail="Account does not exist")

        cookie = acc.get("cookie", "")
        if not cookie:
            raise HTTPException(status_code=400, detail="Account has no configured Cookie")

        payload = self._fetch_self_payload(cookie)
        self_info = self._extract_self_info(payload)
        db.upsert_self_info(account_id, self_info, raw_json=payload)
        return {"self": db.get_self_info(account_id)}

    def refresh_account_self(self, account_id: str) -> Dict[str, Any]:
        sql_mgr = get_accounts_sql_manager()
        acc = sql_mgr.get_account_by_id(account_id, mask_cookie=False)
        if not acc:
            raise HTTPException(status_code=404, detail="Account does not exist")

        cookie = acc.get("cookie", "")
        if not cookie:
            raise HTTPException(status_code=400, detail="Account has no configured Cookie")

        payload = self._fetch_self_payload(cookie)
        self_info = self._extract_self_info(payload)
        db = get_account_info_db()
        db.upsert_self_info(account_id, self_info, raw_json=payload)
        return {"self": db.get_self_info(account_id)}

    def get_group_account_self(self, group_id: str) -> Dict[str, Any]:
        summary = get_account_summary_for_group_auto(group_id)
        cookie = get_cookie_for_group(group_id)
        account_id = (summary or {}).get("id", "default")

        if not cookie:
            raise HTTPException(status_code=400, detail="未找到可用Cookie，请先配置账号或默认Cookie")

        db = get_account_info_db()
        info = db.get_self_info(account_id)
        if info:
            return {"self": info}

        payload = self._fetch_self_payload(cookie)
        self_info = self._extract_self_info(payload)
        db.upsert_self_info(account_id, self_info, raw_json=payload)
        return {"self": db.get_self_info(account_id)}

    def refresh_group_account_self(self, group_id: str) -> Dict[str, Any]:
        summary = get_account_summary_for_group_auto(group_id)
        cookie = get_cookie_for_group(group_id)
        account_id = (summary or {}).get("id", "default")

        if not cookie:
            raise HTTPException(status_code=400, detail="未找到可用Cookie，请先配置账号或默认Cookie")

        payload = self._fetch_self_payload(cookie)
        self_info = self._extract_self_info(payload)
        db = get_account_info_db()
        db.upsert_self_info(account_id, self_info, raw_json=payload)
        return {"self": db.get_self_info(account_id)}
