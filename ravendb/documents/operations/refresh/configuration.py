from typing import Optional


class RefreshConfiguration:
    def __init__(self, disabled: Optional[bool] = None, refresh_frequency_in_sec: Optional[int] = None):
        self.disabled = disabled
        self.refresh_frequency_in_sec = refresh_frequency_in_sec
