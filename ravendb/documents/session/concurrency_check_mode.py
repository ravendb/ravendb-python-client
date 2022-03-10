from enum import Enum


class ConcurrencyCheckMode(Enum):
    AUTO = "AUTO"
    FORCED = "FORCED"
    DISABLED = "DISABLED"

    def __str__(self):
        return self.value
