from fastapi import APIRouter, HTTPException
import requests

from api.schemas.models import AccountCreateRequest, AssignGroupAccountRequest
from api.services.account_service import AccountService

router = APIRouter(tags=["accounts"])
service = AccountService()


@router.get("/api/accounts")
async def list_accounts():
    try:
        return service.list_accounts()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve account list: {str(e)}")


@router.post("/api/accounts")
async def create_account(request: AccountCreateRequest):
    try:
        return service.create_account(request)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create account: {str(e)}")


@router.delete("/api/accounts/{account_id}")
async def remove_account(account_id: str):
    try:
        return service.remove_account(account_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete account: {str(e)}")


@router.post("/api/groups/{group_id}/assign-account")
async def assign_account_to_group(group_id: str, request: AssignGroupAccountRequest):
    try:
        return service.assign_account_to_group(group_id, request)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to assign account: {str(e)}")


@router.get("/api/groups/{group_id}/account")
async def get_group_account(group_id: str):
    try:
        return service.get_group_account(group_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取群组账号失败: {str(e)}")


@router.get("/api/accounts/{account_id}/self")
async def get_account_self(account_id: str):
    try:
        return service.get_account_self(account_id)
    except HTTPException:
        raise
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Network request failed: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve account info: {str(e)}")


@router.post("/api/accounts/{account_id}/self/refresh")
async def refresh_account_self(account_id: str):
    try:
        return service.refresh_account_self(account_id)
    except HTTPException:
        raise
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Network request failed: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to refresh account info: {str(e)}")


@router.get("/api/groups/{group_id}/self")
async def get_group_account_self(group_id: str):
    try:
        return service.get_group_account_self(group_id)
    except HTTPException:
        raise
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"网络请求失败: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取群组账号信息失败: {str(e)}")


@router.post("/api/groups/{group_id}/self/refresh")
async def refresh_group_account_self(group_id: str):
    try:
        return service.refresh_group_account_self(group_id)
    except HTTPException:
        raise
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"网络请求失败: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"刷新群组账号信息失败: {str(e)}")
