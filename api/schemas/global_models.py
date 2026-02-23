from typing import List, Optional

from pydantic import BaseModel, Field


class ScanFilterConfigPayload(BaseModel):
    default_action: str = Field(default="include")
    whitelist_group_ids: List[str] = Field(default_factory=list)
    blacklist_group_ids: List[str] = Field(default_factory=list)


class GlobalTaskResult(BaseModel):
    task_id: str
    message: str
    detail: Optional[str] = None

