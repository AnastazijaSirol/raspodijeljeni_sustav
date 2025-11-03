from pydantic import BaseModel
from typing import Optional

class Reading(BaseModel):
    camera_id: str
    camera_location: str
    vehicle_id: str
    timestamp: Optional[str] = None
    is_entrance: Optional[bool] = None
    is_camera: Optional[bool] = None
    is_restarea: Optional[bool] = None
    speed: Optional[int] = None
    speed_limit: Optional[int] = None
    timestamp_entrance: Optional[str] = None
    timestamp_exit: Optional[str] = None
