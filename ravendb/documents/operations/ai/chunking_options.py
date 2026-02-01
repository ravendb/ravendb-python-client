from enum import Enum
from typing import Dict, Any, Optional, List


class ChunkingMethod(Enum):
    PLAIN_TEXT_SPLIT = "PlainTextSplit"
    PLAIN_TEXT_SPLIT_LINES = "PlainTextSplitLines"
    PLAIN_TEXT_SPLIT_PARAGRAPHS = "PlainTextSplitParagraphs"
    MARK_DOWN_SPLIT_LINES = "MarkDownSplitLines"
    MARK_DOWN_SPLIT_PARAGRAPHS = "MarkDownSplitParagraphs"
    HTML_STRIP = "HtmlStrip"


# Methods that support overlap tokens
METHODS_SUPPORTING_OVERLAP_TOKENS = {
    ChunkingMethod.PLAIN_TEXT_SPLIT,
    ChunkingMethod.PLAIN_TEXT_SPLIT_LINES,
    ChunkingMethod.PLAIN_TEXT_SPLIT_PARAGRAPHS,
}


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
            chunking_method=ChunkingMethod(json_dict["ChunkingMethod"]),
            max_tokens_per_chunk=json_dict.get("MaxTokensPerChunk", None),
            overlap_tokens=json_dict.get("OverlapTokens", None),
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "ChunkingMethod": self.chunking_method.value if self.chunking_method else None,
            "MaxTokensPerChunk": self.max_tokens_per_chunk,
            "OverlapTokens": self.overlap_tokens,
        }

    def validate(self, source: str, errors: List[str]) -> None:
        """Validates the chunking options.

        Args:
            source: The source context for error messages (e.g., 'embeddings.generate').
            errors: List to append validation errors to.
        """
        if self.max_tokens_per_chunk <= 0:
            errors.append(f"{source}: MaxTokensPerChunk must be greater than 0.")

        if self.overlap_tokens < 0:
            errors.append(f"{source}: OverlapTokens cannot be negative.")

        if self.overlap_tokens > self.max_tokens_per_chunk:
            errors.append(f"{source}: OverlapTokens cannot be greater than MaxTokensPerChunk.")

        if self.overlap_tokens > 0 and self.chunking_method not in METHODS_SUPPORTING_OVERLAP_TOKENS:
            errors.append(
                f"{source}: OverlapTokens is only supported for PlainTextSplit, "
                f"PlainTextSplitLines, and PlainTextSplitParagraphs chunking methods."
            )

    @staticmethod
    def are_equal(left: Optional["ChunkingOptions"], right: Optional["ChunkingOptions"]) -> bool:
        """Check if two ChunkingOptions instances are equal, handling None values."""
        if left is None and right is None:
            return True
        if left is None or right is None:
            return False
        return left == right

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
