from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable, List, Dict, Generic, TypeVar, Union, TYPE_CHECKING

from ravendb import constants
from ravendb.documents.session.tokens.query_tokens.definitions import MoreLikeThisToken


_T = TypeVar("_T")

if TYPE_CHECKING:
    from ravendb.documents.session.query import DocumentQuery, AbstractDocumentQuery


class MoreLikeThisBase(ABC):
    def __init__(self):
        self.options: Union[None, MoreLikeThisOptions] = None


class MoreLikeThisUsingAnyDocument(MoreLikeThisBase):
    pass


class MoreLikeThisUsingDocument(MoreLikeThisBase):
    def __init__(self, document_json: str):
        super().__init__()
        self.document_json = document_json


class MoreLikeThisUsingDocumentForDocumentQuery(Generic[_T], MoreLikeThisBase):
    def __init__(self):
        super().__init__()
        self.for_document_query: Union[None, Callable[[AbstractDocumentQuery[_T]], None]] = None


class MoreLikeThisScope:
    def __init__(self, token: MoreLikeThisToken, add_query_parameter: Callable[[object], str], on_dispose: Callable):
        self.__token = token
        self.__add_query_parameter = add_query_parameter
        self.__on_dispose = on_dispose

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.__on_dispose is not None:
            self.__on_dispose()

    def with_options(self, options: MoreLikeThisOptions) -> None:
        if options is None:
            return

        options_as_json = options.to_json()
        self.__token.options_parameter_name = self.__add_query_parameter(options_as_json)

    def with_document(self, document: str) -> None:
        self.__token.document_parameter_name = self.__add_query_parameter(document)


class MoreLikeThisOptions:
    DEFAULT_MAXIMUM_NUMBER_OF_TOKENS_PARSED = 5000
    DEFAULT_MINIMUM_TERM_FREQUENCY = 2
    DEFAULT_MINIMUM_DOCUMENT_FREQUENCY = 5
    DEFAULT_MAXIMUM_DOCUMENT_FREQUENCY = constants.int_max
    DEFAULT_BOOST = False
    DEFAULT_BOOST_FACTOR = 1
    DEFAULT_MINIMUM_WORD_LENGTH = 0
    DEFAULT_MAXIMUM_WORD_LENGTH = 0
    DEFAULT_MAXIMUM_QUERY_TERMS = 25

    @classmethod
    def default_options(cls) -> MoreLikeThisOptions:
        return cls()

    def __init__(
        self,
        minimum_term_frequency: int = None,
        maximum_query_terms: int = None,
        maximum_number_of_tokens_parsed: int = None,
        minimum_word_length: int = None,
        maximum_word_length: int = None,
        minimum_document_frequency: int = None,
        maximum_document_frequency: int = None,
        maximum_document_frequency_percentage: int = None,
        boost: bool = None,
        boost_factor: float = None,
        stop_words_document_id: str = None,
        fields: List[str] = None,
    ):
        self.minimum_term_frequency = minimum_term_frequency
        self.maximum_query_terms = maximum_query_terms
        self.maximum_number_of_tokens_parsed = maximum_number_of_tokens_parsed
        self.minimum_word_length = minimum_word_length
        self.maximum_word_length = maximum_word_length
        self.minimum_document_frequency = minimum_document_frequency
        self.maximum_document_frequency = maximum_document_frequency
        self.maximum_document_frequency_percentage = maximum_document_frequency_percentage
        self.boost = boost
        self.boost_factor = boost_factor
        self.stop_words_document_id = stop_words_document_id
        self.fields = fields

    def to_json(self) -> Dict:
        return {
            "MinimumTermFrequency": self.minimum_term_frequency,
            "MaximumQueryTerms": self.maximum_query_terms,
            "MaximumNumberOfTokensParsed": self.maximum_number_of_tokens_parsed,
            "MinimumWordLength": self.minimum_word_length,
            "MaximumWordLength": self.maximum_word_length,
            "MinimumDocumentFrequency": self.minimum_document_frequency,
            "MaximumDocumentFrequency": self.maximum_document_frequency,
            "MaximumDocumentFrequencyPercentage": self.maximum_document_frequency_percentage,
            "Boost": self.boost,
            "BoostFactor": self.boost_factor,
            "StopWordsDocumentId": self.stop_words_document_id,
            "Fields": self.fields,
        }


class MoreLikeThisOperations(Generic[_T]):
    @abstractmethod
    def with_options(self, options: MoreLikeThisOptions) -> MoreLikeThisOperations[_T]:
        pass


class MoreLikeThisBuilder(Generic[_T], MoreLikeThisOperations[_T]):
    def __init__(self):
        self.__more_like_this: Union[None, MoreLikeThisBase] = None

    @property
    def more_like_this(self) -> MoreLikeThisBase:
        return self.__more_like_this

    def using_any_document(self) -> MoreLikeThisOperations[_T]:
        self.__more_like_this = MoreLikeThisUsingAnyDocument()
        return self

    def using_document(
        self, document_json_or_builder: Union[str, Callable[[DocumentQuery[_T]], None]]
    ) -> MoreLikeThisOperations[_T]:
        if isinstance(document_json_or_builder, str):
            self.__more_like_this = MoreLikeThisUsingDocument(document_json_or_builder)
            return self
        self.__more_like_this = MoreLikeThisUsingDocumentForDocumentQuery()
        self.__more_like_this.for_document_query = document_json_or_builder
        return self

    def with_options(self, options: MoreLikeThisOptions) -> MoreLikeThisOperations[_T]:
        self.__more_like_this.options = options
        return self


class MoreLikeThisStopWords:
    def __init__(self, Id: str = None, stop_words: List[str] = None):
        self.Id = Id
        self.stop_words = stop_words

    def to_json(self):
        return {"Id": self.Id, "StopWords": self.stop_words}
