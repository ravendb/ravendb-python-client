from __future__ import annotations
from typing import Dict, Any


class AiMessagePromptFields:
    TEXT = "text"
    TYPE = "type"
    IMAGE = "image"


class AiMessagePromptTypes:
    TEXT = "text"


class ContentPart:
    def __init__(self, content_type: str):
        self._type = content_type

    @property
    def type(self) -> str:
        return self._type

    def to_json(self) -> Dict[str, Any]:
        return {AiMessagePromptFields.TYPE: self._type}


class TextPart(ContentPart):
    def __init__(self, text: str):
        super().__init__(AiMessagePromptTypes.TEXT)
        self._text = text

    @property
    def text(self) -> str:
        return self._text

    @text.setter
    def text(self, value: str):
        self._text = value

    def to_json(self) -> Dict[str, Any]:
        return {
            AiMessagePromptFields.TYPE: self._type,
            AiMessagePromptFields.TEXT: self._text,
        }
