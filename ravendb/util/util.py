import uuid
from typing import Optional


class RaftIdGenerator:
    def __init__(self):
        pass

    @staticmethod
    def new_id() -> str:
        return str(uuid.uuid4())

    @staticmethod
    def dont_care_id() -> str:
        return ""


class StartingWithOptions:
    def __init__(self, start_with: str, start: Optional[int] = None, page_size: Optional[int] = None):
        self.starts_with = start_with
        self.start = start
        self.page_size = page_size
