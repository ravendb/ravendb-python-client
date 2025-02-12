import inspect
from copy import deepcopy
from typing import Optional, TYPE_CHECKING, Union, Type, TypeVar, Dict, Any, Tuple

from ravendb.primitives import constants
from ravendb.documents.session.document_info import DocumentInfo
from ravendb.documents.session.event_args import (
    BeforeConversionToDocumentEventArgs,
    AfterConversionToDocumentEventArgs,
    BeforeConversionToEntityEventArgs,
    AfterConversionToEntityEventArgs,
)
from ravendb.exceptions import exceptions
from ravendb.exceptions.exceptions import InvalidOperationException
from ravendb.tools.utils import Utils, DynamicStructure
from ravendb.documents.conventions import DocumentConventions

if TYPE_CHECKING:
    from ravendb.documents.session.document_session_operations.in_memory_document_session_operations import (
        InMemoryDocumentSessionOperations,
    )

_T = TypeVar("_T")


class EntityToJson:
    def __init__(self, session: "InMemoryDocumentSessionOperations"):
        self._session = session
        self._missing_dictionary = dict()

    @property
    def missing_dictionary(self):
        return self._missing_dictionary

    # Converting objects to JSON Dicts
    # ================================
    def convert_entity_to_json(self, entity: object, document_info: DocumentInfo) -> dict:
        should_invoke_events = document_info is not None
        if should_invoke_events:
            self._session.before_conversion_to_document_invoke(
                BeforeConversionToDocumentEventArgs(document_info.key, entity, self._session)
            )

        document = EntityToJson._convert_entity_to_json_internal(self, entity, document_info, True)

        if should_invoke_events:
            self._session.after_conversion_to_document_invoke(
                AfterConversionToDocumentEventArgs(self._session, document_info.key, entity, document)
            )
        return document

    def _convert_entity_to_json_internal(
        self, entity: object, document_info: DocumentInfo, remove_identity_property: bool = False
    ) -> dict:
        json_node = Utils.entity_to_dict(entity, self._session.conventions.json_default_method)
        EntityToJsonUtils.write_metadata(json_node, document_info)

        if remove_identity_property:
            EntityToJsonUtils.try_remove_identity_property_from_json_dict(
                json_node, entity.__class__, self._session.conventions
            )

        return json_node

    # Converting JSON Dicts to objects
    # ================================
    def convert_to_entity(self, entity_type: Type[_T], key: str, document: dict, track_entity: bool) -> _T:
        conventions = self._session.conventions
        return EntityToJsonStatic.convert_to_entity(document, entity_type, conventions, self._session, key)

    # Miscellaneous
    # =============
    def populate_entity(self, entity: object, key: str, document: Dict[str, Any]) -> None:
        if key is None:
            raise ValueError("Key cannot be None")

        EntityToJsonStatic.populate_entity(entity, document)
        self._session.generate_entity_id_on_the_client.try_set_identity(entity, key)

    def remove_from_missing(self, entity):
        try:
            del self.missing_dictionary[entity]
        except KeyError:
            pass

    def clear(self):
        self.missing_dictionary.clear()


class EntityToJsonStatic:
    @staticmethod
    def populate_entity(entity: object, document: Dict[str, Any]) -> None:
        if entity is None:
            raise ValueError("Entity cannot be None")
        if document is None:
            raise ValueError("Document cannot be None")
        entity.__dict__.update(document)

    @staticmethod
    def convert_entity_to_json(
        entity: object,
        conventions: "DocumentConventions",
        document_info: Optional[DocumentInfo],
        remove_identity_property: Optional[bool] = False,
    ) -> Dict[str, Any]:
        json_dict = Utils.entity_to_dict(entity, conventions.json_default_method)
        EntityToJsonUtils.write_metadata(json_dict, document_info)

        if remove_identity_property:
            EntityToJsonUtils.try_remove_identity_property_from_json_dict(json_dict, entity.__class__, conventions)

        return json_dict

    @staticmethod
    def convert_to_entity_by_key(
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
                    entity = EntityToJsonStatic.convert_to_entity(document, clazz, conventions)

            if entity is None:
                entity = EntityToJsonStatic.convert_to_entity(document, entity_class, conventions)

            return entity
        except Exception as e:
            raise RuntimeError(f"Could not convert document {key} to entity of type {entity_class}", e)

    @staticmethod
    def convert_to_entity(
        document: Dict[str, Any],
        object_type: Type[_T],
        conventions: "DocumentConventions",
        session: Optional["InMemoryDocumentSessionOperations"] = None,
        key: str = None,
    ) -> _T:
        metadata = document.get("@metadata")
        document_deepcopy = deepcopy(document)

        object_type, is_projection, should_update_metadata_python_type = EntityToJsonUtils.determine_object_type(
            document, conventions, object_type, metadata
        )

        if object_type is dict:
            EntityToJsonUtils.invoke_after_conversion_to_entity_event(session, key, object_type, document_deepcopy)
            return document_deepcopy

        if object_type is DynamicStructure:
            dyn = DynamicStructure(**document_deepcopy)
            EntityToJsonUtils.invoke_after_conversion_to_entity_event(session, key, object_type, document_deepcopy)
            return dyn

        if should_update_metadata_python_type:
            EntityToJsonUtils.set_python_type_in_metadata(metadata, object_type)

        # Fire before conversion to entity events
        if session:
            session.before_conversion_to_entity_invoke(
                BeforeConversionToEntityEventArgs(session, key, object_type, document_deepcopy)
            )

        # Conversion to entity

        if "from_json" in object_type.__dict__ and inspect.ismethod(object_type.from_json):
            # By custom defined 'from_json' serializer class method
            entity = object_type.from_json(document_deepcopy)
        else:
            entity = Utils.convert_json_dict_to_object(document_deepcopy, object_type, is_projection)

        EntityToJsonUtils.invoke_after_conversion_to_entity_event(session, key, object_type, document_deepcopy)

        # Try to set identity property
        identity_property_name = conventions.get_identity_property_name(object_type)
        if identity_property_name in entity.__dict__:
            entity.__dict__[identity_property_name] = metadata.get("@id", None)

        return entity


class EntityToJsonUtils:
    @staticmethod
    def invoke_after_conversion_to_entity_event(
        session: Optional["InMemoryDocumentSessionOperations"],
        key: str,
        object_type: Optional[_T],
        document_deepcopy: dict,
    ):
        if session:
            session.after_conversion_to_entity_invoke(
                AfterConversionToEntityEventArgs(session, key, object_type, document_deepcopy)
            )

    @staticmethod
    def try_remove_identity_property_from_json_dict(
        document: Dict[str, Any], entity_type: Type[Any], conventions: DocumentConventions
    ) -> bool:
        identity_property_name = conventions.get_identity_property_name(entity_type)
        if identity_property_name is None:
            return False

        try:
            del document[identity_property_name]
            return True
        except KeyError:
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
    def determine_object_type(
        document: Dict[str, Any],
        conventions: DocumentConventions,
        object_type_from_user: Optional[Type[Any]] = None,
        metadata: Dict[str, Any] = None,
    ) -> Tuple[
        Type[Union[Any, DynamicStructure, Dict[str, Any]]], bool, bool
    ]:  # -> object_type, is_projection, should_update_metadata_python_type
        # Try to extract the object type from the metadata
        type_name_from_metadata = conventions.try_get_type_from_metadata(metadata)

        # Check if user needs dictionary or if we can return dictionary
        if object_type_from_user is dict or (
            object_type_from_user is None and type_name_from_metadata == "builtins.dict"
        ):
            return dict, False, False

        # No Python type in metadata
        if type_name_from_metadata is None:
            if object_type_from_user is not None:
                # Try using passed object type
                return object_type_from_user, False, True
            else:
                # No type defined, but the user didn't explicitly say that they need a dict - return DynamicStructure
                return DynamicStructure, False, False

        # Python object type is in the metadata
        object_type_from_metadata = Utils.import_class(type_name_from_metadata)
        if object_type_from_metadata is None:
            # Unsuccessful import means the document has been probably stored within other Python project
            # or the original object class has been removed - essentially we have only object_type to rely on
            if object_type_from_user is None:
                raise RuntimeError(
                    f"Cannot import class '{type_name_from_metadata}' to convert '{document}' to an object, "
                    f"it might be removed from your project. Provide an alternative object type "
                    f"to convert the document to or pass 'dict' to receive dictionary JSON representation."
                )
            return object_type_from_user, False, False

        # Successfully imported the class from metadata - but before conversion check for projections and inheritance

        # Maybe user wants to cast from dict to their type
        if object_type_from_metadata is dict:
            return object_type_from_user, False, False

        # User doesn't need projection, or class from metadata is a child of a class provided by user
        # We can safely use class from metadata
        if object_type_from_user is None or Utils.is_inherit(object_type_from_user, object_type_from_metadata):
            return object_type_from_metadata, False, False

        # Passed type is not a type from metadata, neither there's no inheritance - probably projection
        elif object_type_from_user is not object_type_from_metadata:
            # Check if types are compatible
            incompatible_fields = Utils.check_valid_projection(object_type_from_user, object_type_from_metadata)
            if incompatible_fields:
                raise exceptions.InvalidOperationException(
                    f"Invalid projection. Cannot covert document "
                    f"from type '{object_type_from_metadata.__name__}' "
                    f"to type '{object_type_from_user.__name__}'. "
                    f"Type '{object_type_from_user.__name__}' instance has fields {incompatible_fields} "
                    f"that aren't on '{object_type_from_metadata.__name__}'."
                )

            # Projection
            return object_type_from_user, True, False

    @staticmethod
    def set_python_type_in_metadata(metadata: Dict[str, Any], object_type: Type[Any]) -> None:
        metadata["Raven-Python-Type"] = "{0}.{1}".format(object_type.__module__, object_type.__name__)
