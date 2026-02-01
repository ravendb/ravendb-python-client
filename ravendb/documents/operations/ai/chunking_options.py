from enum import Enum
from typing import Dict, Any, Optional


# todo: EmbeddingsGenerationConfiguration
class ChunkingMethod(Enum):
    PLAIN_TEXT_SPLIT = "PlainTextSplit"
    PLAIN_TEXT_SPLIT_LINES = "PlainTextSplitLines"
    PLAIN_TEXT_SPLIT_PARAGRAPHS = "PlainTextSplitParagraphs"
    MARK_DOWN_SPLIT_LINES = "MarkDownSplitLines"
    MARK_DOWN_SPLIT_PARAGRAPHS = "MarkDownSplitParagraphs"
    HTML_STRIP = "HtmlStrip"


class ChunkingOptions:
    def __init__(
        self,
        chunking_method: Optional[ChunkingMethod] = None,
        max_tokens_per_chunk: int = 512,
        overlap_tokens: int = 0,
    ):
        self.chunking_method = chunking_method
        self.max_tokens_per_chunk = max_tokens_per_chunk
        self.overlap_tokens = overlap_tokens

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "ChunkingOptions":
        return cls(
            chunking_method=ChunkingMethod(json_dict["ChunkingMethod"]) if json_dict.get("ChunkingMethod") else None,
            max_tokens_per_chunk=json_dict.get("MaxTokensPerChunk", 512),
            overlap_tokens=json_dict.get("OverlapTokens", 0),
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "ChunkingMethod": self.chunking_method.value if self.chunking_method else None,
            "MaxTokensPerChunk": self.max_tokens_per_chunk,
            "OverlapTokens": self.overlap_tokens,
        }

    def __eq__(self, other: object) -> bool:
        if other is None:
            return False
        if self is other:
            return True
        if not isinstance(other, ChunkingOptions):
            return False
        return (
            self.chunking_method == other.chunking_method
            and self.max_tokens_per_chunk == other.max_tokens_per_chunk
            and self.overlap_tokens == other.overlap_tokens
        )

    def __hash__(self) -> int:
        return hash((self.chunking_method, self.max_tokens_per_chunk, self.overlap_tokens))
