from pydantic import BaseModel
from datetime import datetime

class Reading(BaseModel):
    camera_id: int
    camera_location: str
    vehicle_id: str
    timestamp: str
    is_entrance: bool
    is_camera: bool
    is_restarea: bool
    speed: int
    speed_limit: int
    timestamp_entrance: str
    timestamp_exit: str
