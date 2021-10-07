from datetime import datetime, timedelta
from typing import Optional

from pyravendb import constants
from pyravendb.custom_exceptions import exceptions
from pyravendb.data.document_conventions import DocumentConventions
from pyravendb.documents.session.document_info import DocumentInfo
from pyravendb.documents.session.in_memory_document_session_operations import InMemoryDocumentSessionOperations
from pyravendb.tools.projection import create_entity_with_mapper
from pyravendb.tools.utils import Utils, _DynamicStructure
from copy import deepcopy
import json


class EntityToJson:
    def __init__(self, session: InMemoryDocumentSessionOperations):
        self._session = session
        self._missing_dictionary = dict()

    @property
    def missing_dictionary(self):
        return self._missing_dictionary

    def convert_entity_to_json(self, entity: object, document_info: DocumentInfo) -> dict:
        if document_info is not None:
            self._session.events.before_conversion_to_json_internal(document_info.key, entity)
        document = EntityToJson._convert_entity_to_json_internal(self, entity, document_info)
        if document_info is not None:
            self._session.events.after_conversion_to_document(document_info.key, entity, document)
        return document

    @staticmethod
    def convert_entity_to_json_internal_static(
        entity,
        conventions: DocumentConventions,
        document_info: DocumentInfo,
        remove_identity_property: Optional[bool] = True,
    ) -> dict:
        json_node = Utils.entity_to_dict(entity, conventions.json_default_method)
        EntityToJson.write_metadata(json_node, document_info)
        if remove_identity_property:
            EntityToJson.try_remove_identity_property(entity)
        return json_node

    @staticmethod
    def convert_entity_to_json_static(entity, conventions: DocumentConventions, document_info: DocumentInfo):
        return EntityToJson.convert_entity_to_json_internal_static(entity, conventions, document_info)

    def _convert_entity_to_json_internal(
        self, entity: object, document_info: DocumentInfo, remove_identity_property: bool = False
    ) -> dict:
        json_node = Utils.entity_to_dict(entity, self._session.conventions.json_default_method)
        self.write_metadata(json_node, document_info)
        if remove_identity_property:
            self.try_remove_identity_property(json_node)
        return json_node

    def convert_to_entity(
        self, entity_type: type, key: str, document: dict, track_entity: bool, nested_object_types=None
    ):
        conventions = self._session.conventions
        events = self._session.events
        self.convert_to_entity_static(document, entity_type, conventions, events, nested_object_types)

    @staticmethod
    def populate_entity(entity, document: dict):
        if entity is None:
            raise ValueError("Entity cannot be None")
        if document is None:
            raise ValueError("Document cannot be None")
        entity.__dict__.update(document)

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
    def convert_to_entity_static(document, object_type, conventions, events, nested_object_types=None):
        metadata = document.pop("@metadata")
        original_document = deepcopy(document)
        type_from_metadata = conventions.try_get_type_from_metadata(metadata)
        mapper = conventions.mappers.get(object_type, None)

        events.before_conversion_to_entity(document, metadata, type_from_metadata)

        if object_type == dict:
            events.after_conversion_to_entity(document, document, metadata)
            return document, metadata, original_document

        if type_from_metadata is None:
            if object_type is not None:
                metadata["Raven-Python-Type"] = "{0}.{1}".format(object_type.__module__, object_type.__name__)
            else:  # no type defined on document or during load, return a dict
                dyn = _DynamicStructure(**document)
                events.after_conversion_to_entity(dyn, document, metadata)
                return dyn, metadata, original_document
        else:
            object_from_metadata = Utils.import_class(type_from_metadata)
            if object_from_metadata is not None:
                if object_type is None:
                    object_type = object_from_metadata

                elif Utils.is_inherit(object_type, object_from_metadata):
                    mapper = conventions.mappers.get(object_from_metadata, None) or mapper
                    object_type = object_from_metadata
                elif object_type is not object_from_metadata:
                    # todo: Try to parse if we use projection
                    raise exceptions.InvalidOperationException(
                        f"Cannot covert document from type {object_from_metadata} to {object_type}"
                    )

        if nested_object_types is None and mapper:
            entity = create_entity_with_mapper(document, mapper, object_type)
        else:
            entity = _DynamicStructure(**document)
            entity.__class__ = object_type

            entity = Utils.initialize_object(document, object_type)

            if nested_object_types:
                for key in nested_object_types:
                    attr = getattr(entity, key)
                    if attr:
                        try:
                            if isinstance(attr, list):
                                nested_list = []
                                for attribute in attr:
                                    nested_list.append(Utils.initialize_object(attribute, nested_object_types[key]))
                                setattr(entity, key, nested_list)
                            elif nested_object_types[key] is datetime:
                                setattr(entity, key, Utils.string_to_datetime(attr))
                            elif nested_object_types[key] is timedelta:
                                setattr(entity, key, Utils.string_to_timedelta(attr))
                            else:
                                setattr(
                                    entity,
                                    key,
                                    Utils.initialize_object(attr, nested_object_types[key]),
                                )
                        except TypeError as e:
                            print(e)
                            pass

        if "Id" in entity.__dict__:
            entity.Id = metadata.get("@id", None)
        events.after_conversion_to_entity(entity, document, metadata)
        return entity, metadata, original_document

    def remove_from_missing(self, entity):
        try:
            self.missing_dictionary[entity]
        except KeyError:
            pass

    def clear(self):
        self.missing_dictionary.clear()
