import json
from typing import Dict, Type, TypeVar, Optional

from ravendb import constants
from ravendb.documents.conventions import DocumentConventions
from ravendb.documents.operations.compare_exchange.compare_exchange import CompareExchangeValue
from ravendb.json.metadata_as_dictionary import MetadataAsDictionary
from ravendb.tools.utils import CaseInsensitiveDict, Utils

_T = TypeVar("_T")


class CompareExchangeValueResultParser:
    @staticmethod
    def get_values(
        object_type: Type[_T], response: str, materialize_metadata: bool, conventions: DocumentConventions
    ) -> Dict[str, CompareExchangeValue[_T]]:
        results = CaseInsensitiveDict()
        if not response:
            return results
        items = json.loads(response)["Results"]
        if not items:
            raise ValueError("Response is invalid. Results is missing.")

        for item in items:
            if not item:
                raise ValueError("Response is invalid. Item is None")
            value = CompareExchangeValueResultParser.get_single_value(
                object_type, item, materialize_metadata, conventions
            )
            results[value.key] = value

        return results

    # todo: check if we can't set object type as optional
    @staticmethod
    def get_value(
        object_type: Type[_T], response: str, materialize_metadata: bool, conventions: DocumentConventions
    ) -> Optional[CompareExchangeValue[_T]]:
        if not response or response == "null":
            return None

        values = CompareExchangeValueResultParser.get_values(object_type, response, materialize_metadata, conventions)
        if not values:
            return None
        return values.values().__iter__().__next__()

    @staticmethod
    def get_single_value(
        object_type: Type[_T], item: dict, materialize_metadata: bool, conventions: DocumentConventions
    ) -> Optional[CompareExchangeValue[_T]]:
        if item is None:
            return None
        if "Key" not in item:
            raise KeyError("Response is invalid. Key is missing.")
        if "Index" not in item:
            raise IndexError("Response is invalid. Index is missing.")
        if "Value" not in item:
            raise ValueError("Response is invalid. Value is missing.")

        key: str = item.get("Key")
        index: int = item.get("Index")
        raw: dict = item.get("Value")
        if not raw:
            return CompareExchangeValue(key, index, None)
        metadata = None
        bjro = raw.get(constants.Documents.Metadata.KEY)
        if bjro:
            metadata = (
                MetadataAsDictionary(bjro)
                if not materialize_metadata
                else MetadataAsDictionary.materialize_from_json(bjro)
            )
        if object_type in Utils.primitives_and_collections:
            value = None
            if raw:
                raw_value = raw.get("Object")
                value = (
                    Utils.convert_json_dict_to_object(raw_value, object_type)
                    if not isinstance(raw_value, Utils.primitives_and_collections)
                    else raw_value
                )

            return CompareExchangeValue(key, index, value, metadata)
        elif object_type == dict:
            if not raw or constants.CompareExchange.OBJECT_FIELD_NAME not in raw:
                return CompareExchangeValue(key, index, None, metadata)

            raw_value = raw.get(constants.CompareExchange.OBJECT_FIELD_NAME)
            if not raw:
                return CompareExchangeValue(key, index, None, metadata)
            else:
                return CompareExchangeValue(key, index, raw_value, metadata)
        else:
            obj = raw.get(constants.CompareExchange.OBJECT_FIELD_NAME)
            if not obj:
                return CompareExchangeValue(key, index, None, metadata)
            else:
                converted = (
                    Utils.convert_json_dict_to_object(obj, object_type)
                    if not isinstance(obj, Utils.primitives_and_collections)
                    else obj
                )
                return CompareExchangeValue(key, index, converted, metadata)
