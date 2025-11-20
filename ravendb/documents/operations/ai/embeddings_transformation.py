from typing import Dict, Any, Optional

from ravendb.documents.operations.ai.chunking_options import ChunkingOptions, ChunkingMethod


# todo: EmbeddingsGenerationConfiguration
class EmbeddingsTransformation:
    GENERATE_EMBEDDINGS_FUNCTION_NAME = "embeddings.generate"

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
        return cls(
            script=json_dict.get("Script"),
            chunking_options=(
                ChunkingOptions.from_json(json_dict["ChunkingOptions"]) if json_dict.get("ChunkingOptions") else None
            ),
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "Script": self.script,
            "ChunkingOptions": self.chunking_options.to_json() if self.chunking_options else None,
        }
