from typing import Optional


class AnalyzerDefinition:
    def __init__(self, name: Optional[str] = None, code: Optional[str] = None):
        self.name = name
        self.code = code
