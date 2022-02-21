from typing import Optional


class ExpirationConfiguration:
    def __init__(self, disabled: Optional[bool] = None, delete_frequency_in_sec: Optional[int] = None):
        self.disabled = disabled
        self.delete_frequency_in_sec = delete_frequency_in_sec
