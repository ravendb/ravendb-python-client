import datetime
from enum import Enum
from typing import Union, Optional

from pyravendb import constants
from pyravendb.data.document_conventions import DocumentConventions
from pyravendb.documents.commands.batches import PutCompareExchangeCommandData, DeleteCompareExchangeCommandData
from pyravendb.exceptions.raven_exceptions import RavenException
from pyravendb.json.metadata_as_dictionary import MetadataAsDictionary
import pyravendb.store.entity_to_json
from pyravendb.tools.projection import create_entity_with_mapper
from pyravendb.tools.utils import Utils


class CompareExchangeValueState(Enum):
    NONE = "none"
    CREATED = "created"
    DELETED = "deleted"
    MISSING = "missing"

    def __str__(self):
        return self.value


class CompareExchangeValue:
    def __init__(self, key: str, index: int, value, metadata: MetadataAsDictionary = None):
        self.key = key
        self.index = index
        self.value = value
        self._metadata_as_dictionary = metadata

    @property
    def metadata_as_dictionary(self) -> MetadataAsDictionary:
        if not self._metadata_as_dictionary:
            self._metadata_as_dictionary = MetadataAsDictionary()
        return self._metadata_as_dictionary

    @property
    def has_metadata(self) -> bool:
        return self._metadata_as_dictionary is not None


class CompareExchangeSessionValue:
    def __init__(
        self,
        key: str = None,
        index: int = None,
        state: CompareExchangeValueState = None,
        value: Optional[CompareExchangeValue] = None,
    ):
        if not ((value is not None) ^ all([key, index, state])):
            raise ValueError("Specify key, index and state or just pass CompareExchangeValue.")
        if value:
            self.__init__(
                value.key,
                value.index,
                CompareExchangeValueState.NONE if value.index >= 0 else CompareExchangeValueState.MISSING,
            )
            return
        self._key = key
        self._index = index
        self._state = state
        self.__original_value: Union[CompareExchangeValue, None] = None
        self.__value: Union[CompareExchangeValue, None] = None

    def get_value(self, object_type: type, conventions: DocumentConventions) -> Union[None, CompareExchangeValue]:
        if (
            self._state.value == CompareExchangeValueState.CREATED
            or self._state.value == CompareExchangeValueState.NONE
        ):
            if isinstance(self.__value, CompareExchangeValue):
                return self.__value

            if not self.__value:
                raise ValueError("Value cannot be None")

            entity = None
            if self.__original_value is not None and self.__original_value.value is not None:
                if object_type in [int, float, str, bool]:
                    try:
                        entity_json_value = self.__original_value.value.get(constants.CompareExchange.OBJECT_FIELD_NAME)
                        entity = create_entity_with_mapper(
                            entity_json_value, conventions.mappers.get(object_type), object_type, True
                        )
                    except BaseException as ex:
                        raise RavenException(
                            f"Unable to read compare exchange value: {self.__original_value.value}", ex
                        )
                else:
                    entity = create_entity_with_mapper(
                        self.__original_value.value, conventions.mappers.get(object_type), object_type, True
                    )

                    value = CompareExchangeValue(self._key, self._index, entity)
                    self.__value = value
                    return value
        elif (
            self._state.value == CompareExchangeValueState.MISSING
            or self._state.value == CompareExchangeValueState.DELETED
        ):
            return None
        else:
            raise ValueError(f"Not supported state: {self._state}")

    def create(self, item) -> CompareExchangeValue:
        self.__assert_state()
        if self.__value:
            raise RuntimeError(f"The compare exchange value with key {self._key} is already tracked.")
        self._index = 0
        value = CompareExchangeValue(self._key, self._index, item)
        self.__value = value
        self._state = CompareExchangeValueState.CREATED
        return value

    def delete(self, index: int) -> None:
        self.__assert_state()
        self._index = index
        self._state = CompareExchangeValueState.DELETED

    def __assert_state(self) -> None:
        if self._state == CompareExchangeValueState.NONE or CompareExchangeValueState.MISSING:
            return
        elif self._state == CompareExchangeValueState.CREATED:
            raise RuntimeError(f"The compare exchange value with key {self._key} was already stored.")
        elif self._state == CompareExchangeValueState.DELETED:
            raise RuntimeError(f"The compare exchange value with key {self._key} was already deleted")

    def get_command(self, conventions: DocumentConventions):
        s = self._state
        if s == CompareExchangeValueState.NONE or CompareExchangeValueState.CREATED:
            if not self.__value:
                return None

            entity = pyravendb.store.entity_to_json.EntityToJson.convert_entity_to_json_static(
                self.__value.value, conventions, None
            )

            entity_json = entity if isinstance(entity, dict) else None
            metadata = None
            if self.__value.has_metadata and len(self.__value.metadata_as_dictionary) != 0:
                metadata = self.prepare_metadata_for_put(self._key, self.__value.metadata_as_dictionary, conventions)
            entity_to_insert = None
            if not entity_json:
                entity_json = entity_to_insert = self.__convert_entity(self._key, entity, metadata)

            new_value = CompareExchangeValue(self._key, self._index, entity_json)
            has_changed = self.__original_value is None or self.has_changed(self.__original_value, new_value)
            self.__original_value = new_value
            if not has_changed:
                return None

            if entity_to_insert == None:
                entity_to_insert = self.__convert_entity(self._key, entity, metadata)

            return PutCompareExchangeCommandData(new_value.key, entity_to_insert, new_value.index)
        elif s == CompareExchangeValueState.DELETED:
            return DeleteCompareExchangeCommandData(self._key, self._index)
        elif s == CompareExchangeValueState.MISSING:
            return None
        else:
            raise ValueError(f"Not supported state: {self._state}")

    def __convert_entity(self, key: str, entity, metadata):
        object_node = {constants.CompareExchange.OBJECT_FIELD_NAME: Utils.entity_to_dict(entity, self)}
        if metadata:
            object_node[constants.Documents.Metadata.KEY] = metadata
        return object_node

    def has_changed(self, original_value: CompareExchangeValue, new_value: CompareExchangeValue):
        if original_value == new_value:
            return False

        if original_value.key.lower() != new_value.key.lower():
            raise ValueError(f"Keys do not match. Expected '{original_value.key}' but was: {new_value.key}'")

        if original_value.index != new_value.index:
            return True

        return original_value.value != new_value.value

    def update_state(self, index: int) -> None:
        self._index = index
        self._state = CompareExchangeValueState.NONE

        if self.__original_value is not None:
            self.__original_value.index = index

        if self.__value is not None:
            self.__value.index = index

    def update_value(self, value: CompareExchangeValue):
        self._index = value.index
        self._state = CompareExchangeValueState.NONE if value.index >= 0 else CompareExchangeValueState.MISSING
        self.__original_value = value

        if self.__value is not None:
            self.__value.index = self._index

            if self.__value.value is not None:
                pyravendb.store.entity_to_json.EntityToJson.populate_entity(self.__value.value, value.value)

    @staticmethod
    def prepare_metadata_for_put(
        key: str, metadata_dictionary: MetadataAsDictionary, conventions: DocumentConventions
    ) -> dict:
        if constants.Documents.Metadata.EXPIRES in metadata_dictionary:
            obj = metadata_dictionary.get(constants.Documents.Metadata.EXPIRES)
            if not obj:
                raise ValueError(
                    f"The values of {constants.Documents.Metadata.EXPIRES} metadata for compare exchange '{key}' is None"
                )
            if not isinstance(obj, datetime.datetime) and not isinstance(obj, str):
                raise ValueError(
                    f"The type of {constants.Documents.Metadata.EXPIRES} metadata for compare exchange '{key}' is not valid. Use the following type: datetime.datetime or string"
                )

        return create_entity_with_mapper(metadata_dictionary, conventions.mappers[dict], dict, True)
