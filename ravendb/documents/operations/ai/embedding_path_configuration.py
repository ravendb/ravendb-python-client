from typing import Dict, Any, Optional

from ravendb.documents.operations.ai.chunking_options import ChunkingOptions


# todo: EmbeddingsGenerationConfiguration
class EmbeddingPathConfiguration:
    def __init__(self, path: Optional[str] = None, chunking_options: Optional[ChunkingOptions] = None):
        self.path = path
        self.chunking_options = chunking_options

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "EmbeddingPathConfiguration":
        return cls(
            path=json_dict.get("Path"),
            chunking_options=(
                ChunkingOptions.from_json(json_dict["ChunkingOptions"]) if json_dict.get("ChunkingOptions") else None
            ),
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "Path": self.path,
            "ChunkingOptions": self.chunking_options.to_json() if self.chunking_options else None,
        }
