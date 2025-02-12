from __future__ import annotations

from typing import Dict, Any

from ravendb.documents.indexes.vector.embedding import VectorEmbeddingType
from ravendb.primitives import constants


class VectorOptions:
    def __init__(
        self,
        source_embedding_type: VectorEmbeddingType = constants.VectorSearch.DEFAULT_EMBEDDING_TYPE,
        destination_embedding_type: VectorEmbeddingType = constants.VectorSearch.DEFAULT_EMBEDDING_TYPE,
        dimensions: int = None,
        number_of_edges: int = None,
        number_of_candidates_for_indexing: int = None,
    ):
        self.dimensions = dimensions
        self.source_embedding_type = source_embedding_type
        self.destination_embedding_type = destination_embedding_type
        self.numbers_of_candidates_for_indexing = number_of_candidates_for_indexing
        self.number_of_edges = number_of_edges

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> VectorOptions:
        return cls(
            json_dict["SourceEmbeddingType"],
            json_dict["DestinationEmbeddingType"],
            json_dict["Dimensions"],
            json_dict["NumberOfEdges"],
            json_dict["NumberOfCandidatesForIndexing"],
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "SourceEmbeddingType": self.source_embedding_type.value,
            "DestinationEmbeddingType": self.destination_embedding_type.value,
            "Dimensions": self.dimensions,
            "NumberOfCandidatesForIndexing": self.numbers_of_candidates_for_indexing,
            "NumberOfEdges": self.number_of_edges,
        }


class AutoVectorOptions(VectorOptions):
    def __init__(
        self,
        source_embedding_type: VectorEmbeddingType = constants.VectorSearch.DEFAULT_EMBEDDING_TYPE,
        destination_embedding_type: VectorEmbeddingType = constants.VectorSearch.DEFAULT_EMBEDDING_TYPE,
        dimensions: int = None,
        number_of_edges: int = None,
        number_of_candidates_for_indexing: int = None,
        source_field_name: str = None,
    ):
        super().__init__(
            source_embedding_type,
            destination_embedding_type,
            dimensions,
            number_of_edges,
            number_of_candidates_for_indexing,
        )
        self.source_field_name = source_field_name

    @classmethod
    def from_vector_options(cls, vector_options: VectorOptions):
        return cls(
            source_embedding_type=vector_options.source_embedding_type,
            destination_embedding_type=vector_options.destination_embedding_type,
            dimensions=vector_options.dimensions,
            number_of_edges=vector_options.number_of_edges,
            number_of_candidates_for_indexing=vector_options.numbers_of_candidates_for_indexing,
        )

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]):
        vec_options = super().from_json(json_dict)
        auto_vect_options = cls.from_vector_options(vec_options)
        auto_vect_options.source_field_name = json_dict["SourceFieldName"]
        return auto_vect_options

    def to_json(self) -> Dict[str, Any]:
        json_dict = super().to_json()
        json_dict["SourceFieldName"] = self.source_field_name
        return json_dict
