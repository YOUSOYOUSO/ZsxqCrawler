from typing import Any, Dict, Optional

from pydantic import BaseModel


class TaskSnapshot(BaseModel):
    task_id: str
    type: str
    status: str
    message: str
    result: Optional[Dict[str, Any]] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

