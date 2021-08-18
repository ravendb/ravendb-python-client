from pyravendb import constants
from pyravendb.tools.utils import Utils
from copy import deepcopy
import json


class EntityToJson:
    def __init__(self, session):
        self._session = session
        self._missing_dictionary = dict()

    @property
    def missing_dictionary(self):
        return self._missing_dictionary

    def convert_entity_to_json(self, entity, document_info):
        if document_info is not None:
            self._session.events.before_conversion_to_json_internal(document_info["key"], entity)
        document = EntityToJson._convert_entity_to_json_internal(self, entity, document_info)
        if document_info is not None:
            self._session.events.after_conversion_to_document(document_info["key"], entity, document)
        return document

    def _convert_entity_to_json_internal(self, entity, document_info, remove_identity_property=False):
        json_node = Utils.entity_to_dict(entity, self._session.conventions.json_default_method)
        self.write_metadata(json_node, document_info)
        if remove_identity_property:
            self.try_remove_identity_property(json_node)
        return json_node

    @staticmethod
    def try_remove_identity_property(document):
        try:
            del document.Id
            return True
        except AttributeError:
            return False

    @staticmethod
    def write_metadata(json_node, document_info):
        if document_info is None:
            return
        set_metadata = False
        metadata_node = {}

        if document_info.get("metadata") and len(document_info.get("metadata")) > 0:
            set_metadata = True
            for name, value in document_info.get("metadata").items():
                metadata_node.update({name: deepcopy(value)})
        elif document_info.get("metadata_instance"):
            set_metadata = True
            for key, value in document_info.get("metadata_instance").items():
                metadata_node.update({key: value})

        if document_info.get("collection"):
            set_metadata = True
            metadata_node.update({constants.Documents.Metadata.COLLECTION: document_info["collection"]})

        if set_metadata:
            json_node.update({constants.Documents.Metadata.KEY: metadata_node})

    def remove_from_missing(self, entity):
        try:
            self.missing_dictionary[entity]
        except KeyError:
            pass

    def clear(self):
        self.missing_dictionary.clear()
