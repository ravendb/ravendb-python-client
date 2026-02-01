import re
from typing import Dict, Any, Optional, List

from ravendb.documents.operations.ai.chunking_options import ChunkingOptions, ChunkingMethod


class EmbeddingsTransformation:
    GENERATE_EMBEDDINGS_FUNCTION_NAME = "embeddings.generate"
    _EMBEDDINGS_GENERATE_REGEX = re.compile(GENERATE_EMBEDDINGS_FUNCTION_NAME)

    def __init__(
        self,
        script: Optional[str] = None,
        chunking_options: Optional[ChunkingOptions] = None,
    ):
        self.script = script
        self.chunking_options = chunking_options or ChunkingOptions(
            chunking_method=ChunkingMethod.PLAIN_TEXT_SPLIT, max_tokens_per_chunk=256
        )

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "EmbeddingsTransformation":
        chunking_data = json_dict.get("ChunkingOptions")
        return cls(
            script=json_dict["Script"],
            chunking_options=ChunkingOptions.from_json(chunking_data) if chunking_data else None,
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "Script": self.script,
            "ChunkingOptions": self.chunking_options.to_json() if self.chunking_options else None,
        }

    def validate(self, errors: List[str]) -> None:
        """Validates the transformation script and chunking options."""
        self._validate_script(errors)
        if self.chunking_options:
            self.chunking_options.validate(self.GENERATE_EMBEDDINGS_FUNCTION_NAME, errors)

    def _validate_script(self, errors: List[str]) -> None:
        """Validates that the script contains the required embeddings.generate function."""
        if not self.script or not self._EMBEDDINGS_GENERATE_REGEX.search(self.script):
            errors.append(f"Transformation script must use {self.GENERATE_EMBEDDINGS_FUNCTION_NAME} method.")

    @staticmethod
    def are_equal(left: Optional["EmbeddingsTransformation"], right: Optional["EmbeddingsTransformation"]) -> bool:
        """Check if two EmbeddingsTransformation instances are equal, handling None values."""
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
        if not isinstance(other, EmbeddingsTransformation):
            return False
        return self.script == other.script and ChunkingOptions.are_equal(self.chunking_options, other.chunking_options)

    def __hash__(self) -> int:
        return hash((self.script, self.chunking_options))
