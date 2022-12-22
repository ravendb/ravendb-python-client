import inspect
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Optional, TYPE_CHECKING, Union, Type, TypeVar, Dict

from ravendb import constants
from ravendb.documents.session.document_info import DocumentInfo
from ravendb.documents.session.event_args import (
    BeforeConversionToDocumentEventArgs,
    AfterConversionToDocumentEventArgs,
    BeforeConversionToEntityEventArgs,
    AfterConversionToEntityEventArgs,
)
from ravendb.exceptions import exceptions
from ravendb.exceptions.exceptions import InvalidOperationException
from ravendb.tools.utils import Utils, _DynamicStructure
from ravendb.documents.conventions import DocumentConventions

if TYPE_CHECKING:
    from ravendb.documents.session.in_memory_document_session_operations import InMemoryDocumentSessionOperations


_T = TypeVar("_T")


class EntityToJson:
    def __init__(self, session: "InMemoryDocumentSessionOperations"):
        self._session = session
        self._missing_dictionary = dict()

    @property
    def missing_dictionary(self):
        return self._missing_dictionary

    def convert_entity_to_json(self, entity: object, document_info: DocumentInfo) -> dict:
        if document_info is not None:
            self._session.before_conversion_to_document_invoke(
                BeforeConversionToDocumentEventArgs(document_info.key, entity, self._session)
            )
        document = EntityToJson._convert_entity_to_json_internal(self, entity, document_info)
        if document_info is not None:
            self._session.after_conversion_to_document_invoke(
                AfterConversionToDocumentEventArgs(self._session, document_info.key, entity, document)
            )
        return document

    @staticmethod
    def convert_entity_to_json_internal_static(
        entity,
        conventions: "DocumentConventions",
        document_info: Union[None, DocumentInfo],
        remove_identity_property: Optional[bool] = True,
    ) -> dict:
        json_node = Utils.entity_to_dict(entity, conventions.json_default_method)
        EntityToJson.write_metadata(json_node, document_info)
        if remove_identity_property:
            EntityToJson.try_remove_identity_property(entity)
        return json_node

    @staticmethod
    def convert_entity_to_json_static(
        entity, conventions: "DocumentConventions", document_info: Union[None, DocumentInfo]
    ):
        return EntityToJson.convert_entity_to_json_internal_static(entity, conventions, document_info)

    def _convert_entity_to_json_internal(
        self, entity: object, document_info: DocumentInfo, remove_identity_property: bool = False
    ) -> dict:
        json_node = Utils.entity_to_dict(entity, self._session.conventions.json_default_method)
        self.write_metadata(json_node, document_info)
        if remove_identity_property:
            self.try_remove_identity_property(json_node)
        return json_node

    # todo: refactor this method, make it more useful/simple and less ugly (like this return...[0])
    def convert_to_entity(self, entity_type: Type[_T], key: str, document: dict, track_entity: bool) -> _T:
        conventions = self._session.conventions
        return self.convert_to_entity_static(document, entity_type, conventions, self._session)

    @staticmethod
    def populate_entity_static(entity, document: dict) -> None:
        if entity is None:
            raise ValueError("Entity cannot be None")
        if document is None:
            raise ValueError("Document cannot be None")
        entity.__dict__.update(document)

    def populate_entity(self, entity, key: str, document: dict) -> None:
        if key is None:
            raise ValueError("Key cannot be None")

        EntityToJson.populate_entity_static(entity, document)
        self._session.generate_entity_id_on_the_client.try_set_identity(entity, key)

    @staticmethod
    def try_remove_identity_property(document):
        try:
            del document.Id
            return True
        except AttributeError:
            return False

    @staticmethod
    def write_metadata(json_node: dict, document_info: DocumentInfo):
        if document_info is None:
            return
        set_metadata = False
        metadata_node = {}

        if document_info.metadata and len(document_info.metadata) > 0:
            set_metadata = True
            for name, value in document_info.metadata.items():
                metadata_node.update({name: deepcopy(value)})
        elif document_info.metadata_instance:
            set_metadata = True
            for key, value in document_info.metadata_instance.items():
                metadata_node.update({key: value})

        if document_info.collection:
            set_metadata = True
            metadata_node.update({constants.Documents.Metadata.COLLECTION: document_info.collection})

        if set_metadata:
            json_node.update({constants.Documents.Metadata.KEY: metadata_node})

    @staticmethod
    def convert_to_entity_by_key_static(
        entity_class: Type[_T], key: str, document: Dict, conventions: DocumentConventions
    ) -> _T:
        if entity_class is None:
            return document
        try:
            default_value = Utils.get_default_value(entity_class)
            entity = default_value

            document_type = conventions.get_python_class(key, document)
            if document_type is not None:
                clazz = Utils.import_class(document_type)
                if clazz is not None and issubclass(clazz, entity_class):
                    entity = EntityToJson.convert_to_entity_static(document, clazz, conventions)

            if entity is None:
                entity = EntityToJson.convert_to_entity_static(document, entity_class, conventions)

            return entity
        except Exception as e:
            raise RuntimeError(f"Could not convert document {key} to entity of type {entity_class}", e)

    @staticmethod
    def _invoke_after_conversion_to_entity_event(
        session_hook: Optional["InMemoryDocumentSessionOperations"],
        key: str,
        object_type: Optional[_T],
        document_deepcopy: dict,
    ):
        if session_hook:
            session_hook.after_conversion_to_entity_invoke(
                AfterConversionToEntityEventArgs(session_hook, key, object_type, document_deepcopy)
            )

    @staticmethod
    def convert_to_entity_static(
        document: dict,
        object_type: [_T],
        conventions: "DocumentConventions",
        session_hook: Optional["InMemoryDocumentSessionOperations"] = None,
    ) -> _T:
        # This method has two steps - extract the type (I), and then convert it into the entity (II)
        # todo: Separate it into two different functions and isolate the return statements from the first part

        # I. Extract the object type
        metadata = document.pop("@metadata")
        document_deepcopy = deepcopy(document)

        # 1. Get type from metadata
        type_from_metadata = conventions.try_get_type_from_metadata(metadata)
        is_projection = False
        key = metadata.get(constants.Documents.Metadata.ID, None)

        # Fire before conversion to entity events
        if session_hook:
            session_hook.before_conversion_to_entity_invoke(
                BeforeConversionToEntityEventArgs(session_hook, key, object_type, document_deepcopy)
            )

        # 1.1 Check if passed object type (or extracted from metadata) is a dictionary
        if object_type == dict or type_from_metadata == "builtins.dict":
            EntityToJson._invoke_after_conversion_to_entity_event(session_hook, key, object_type, document_deepcopy)
            return document_deepcopy

        # 1.2 If there's no object type in metadata
        if type_from_metadata is None:
            # 1.2.1 Try to set it with passed object type
            if object_type is not None:
                metadata["Raven-Python-Type"] = "{0}.{1}".format(object_type.__module__, object_type.__name__)
            # 1.2.2 no type defined on document or during load, return a dict
            else:
                dyn = _DynamicStructure(**document_deepcopy)
                EntityToJson._invoke_after_conversion_to_entity_event(session_hook, key, object_type, document_deepcopy)
                return dyn

        # 2. There was a type in the metadata
        else:
            object_from_metadata = Utils.import_class(type_from_metadata)
            # 2.1 Import was successful
            if object_from_metadata is not None:
                # 2.1.1 Set object_type to successfully imported type/ from metadata inherits from passed object_type
                if object_type is None or Utils.is_inherit(object_type, object_from_metadata):
                    object_type = object_from_metadata

                # 2.1.2 Passed type is not a type from metadata, neither there's no inheritance - probably projection
                elif object_type is not object_from_metadata:
                    is_projection = True
                    if not all([name in object_from_metadata.__dict__ for name in object_type.__dict__]):
                        raise exceptions.InvalidOperationException(
                            f"Cannot covert document from type {object_from_metadata} to {object_type}"
                        )

        # We have object type set - it was either extracted or passed through args

        # II. Conversion to entity part

        # By custom defined 'from_json' serializer class method
        # todo: make separate interface to do from_json
        if "from_json" in object_type.__dict__ and inspect.ismethod(object_type.from_json):
            entity = object_type.from_json(document_deepcopy)

        # By projection
        elif is_projection:
            entity = _DynamicStructure(**document_deepcopy)
            entity.__class__ = object_type
            try:
                entity = Utils.initialize_object(document_deepcopy, object_type)
            except TypeError as e:
                raise InvalidOperationException("Probably projection error", e)

        # Happy path - successful extraction of the type from metadata, if not - got object_type passed to arguments
        else:
            entity = Utils.convert_json_dict_to_object(document_deepcopy, object_type)

        EntityToJson._invoke_after_conversion_to_entity_event(session_hook, key, object_type, document_deepcopy)

        # Try to set Id
        if "Id" in entity.__dict__:
            entity.Id = metadata.get("@id", None)

        return entity

    def remove_from_missing(self, entity):
        try:
            self.missing_dictionary[entity]
        except KeyError:
            pass

    def clear(self):
        self.missing_dictionary.clear()
