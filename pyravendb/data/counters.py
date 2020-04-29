from enum import Enum


class CounterOperationType(Enum):
    none = "None"
    increment = "Increment"
    delete = "Delete"
    get = "Get"

    def __str__(self):
        return self.value
