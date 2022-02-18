from __future__ import annotations
from typing import Callable


class IncludesUtil:
    @staticmethod
    def include(document: dict, include: str, load_id: Callable[[str], None]):
        if not include or document is None:
            return

    @staticmethod
    def requires_quotes(include: str) -> (bool, str):
        for ch in include:
            if not ch.isalnum() and ch != "_" and ch != ".":
                return True, include.replace("'", "\\'")
        return False, include
