import datetime
from enum import Enum


class ResponseDisposeHandling(Enum):
    MANUALLY = "Manually"
    AUTOMATIC = "Automatic"

    def __str__(self):
        return self.value


# --------- CACHE ------------


class AggressiveCacheMode(Enum):
    TRACK_CHANGES = "TrackChanges"
    DO_NOT_TRACK_CHANGES = "DoNotTrackChanges"


# -------- OPTIONS -----------


class AggressiveCacheOptions:
    def __init__(self, duration: datetime.timedelta, mode: AggressiveCacheMode):
        self.duration = duration
        self.mode = mode
