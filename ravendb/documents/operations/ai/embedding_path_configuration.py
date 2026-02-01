from typing import Dict, Any, Optional

from ravendb.documents.operations.ai.chunking_options import ChunkingOptions


class EmbeddingPathConfiguration:
    def __init__(self, path: Optional[str] = None, chunking_options: Optional[ChunkingOptions] = None):
        self.path = path
        self.chunking_options = chunking_options

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "EmbeddingPathConfiguration":
        chunking_data = json_dict.get("ChunkingOptions")
        return cls(
            path=json_dict["Path"],
            chunking_options=ChunkingOptions.from_json(chunking_data) if chunking_data else None,
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "Path": self.path,
            "ChunkingOptions": self.chunking_options.to_json() if self.chunking_options else None,
        }

    @staticmethod
    def are_equal(left: Optional["EmbeddingPathConfiguration"], right: Optional["EmbeddingPathConfiguration"]) -> bool:
        """Check if two EmbeddingPathConfiguration instances are equal, handling None values."""
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
        if not isinstance(other, EmbeddingPathConfiguration):
            return False
        return self.path == other.path and ChunkingOptions.are_equal(self.chunking_options, other.chunking_options)

    def __hash__(self) -> int:
        return hash((self.path, self.chunking_options))
