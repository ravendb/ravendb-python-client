from __future__ import annotations
from typing import Dict, Any


class AiMessagePromptFields:
    """Constants for AI message prompt field names."""

    TEXT = "text"
    TYPE = "type"


class AiMessagePromptTypes:
    """Constants for AI message prompt types."""

    TEXT = "text"


class ContentPart:
    """
    Base class for content parts in AI prompts.
    Content parts allow structured prompt content with different types (text, etc.).
    """

    def __init__(self, content_type: str):
        self._type = content_type

    @property
    def type(self) -> str:
        return self._type

    def to_json(self) -> Dict[str, Any]:
        """
        Converts the content part to a JSON-serializable dictionary.
        Subclasses should override this method to include their specific fields.
        """
        return {AiMessagePromptFields.TYPE: self._type}


class TextPart(ContentPart):
    """
    Represents a text content part in AI prompts.
    """

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
