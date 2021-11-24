from pyravendb.json.json_operation import JsonOperation
from pyravendb.json.metadata_as_dictionary import MetadataAsDictionary
from pyravendb.tools.generate_id import GenerateEntityIdOnTheClient
from pyravendb.store.session_query import Query
from pyravendb.commands import commands_data
from pyravendb.commands.raven_commands import *
from pyravendb.raven_operations.operations import GetAttachmentOperation
from pyravendb.commands.commands_data import (
    PutAttachmentCommandData,
    DeleteAttachmentCommandData,
    DeleteCommandData,
)
from pyravendb.tools.utils import Utils, CaseInsensitiveDict
from pyravendb.data.operation import AttachmentType
from pyravendb.data.timeseries import TimeSeriesRangeResult
from .session_timeseries import TimeSeries
from .session_counters import DocumentCounters
from pyravendb.store.entity_to_json import EntityToJson
from pyravendb import constants
from collections import MutableSet
from typing import Dict, List
from copy import deepcopy
from itertools import chain
import json


class _SaveChangesData(object):
    def __init__(self, commands, deferred_command_count, entities=None):
        self.commands = commands
        self.entities = [] if entities is None else entities
        self.deferred_command_count = deferred_command_count


class _RefEq:
    def __init__(self, ref):
        if isinstance(ref, _RefEq):
            self.ref = ref.ref
            return
        self.ref = ref

    # As we split the hashable and unhashable items into separate collections, we only compare _RefEq to other _RefEq
    def __eq__(self, other):
        if isinstance(other, _RefEq):
            return id(self.ref) == id(other.ref)
        raise TypeError("Expected _RefEq type object")

    def __hash__(self):
        return id(self.ref)


class _RefEqEntityHolder(object):
    def __init__(self):
        self.unhashable_items = dict()

    def __len__(self):
        return len(self.unhashable_items)

    def __contains__(self, item):
        return _RefEq(item) in self.unhashable_items

    def __delitem__(self, key):
        del self.unhashable_items[_RefEq(key)]

    def __setitem__(self, key, value):
        self.unhashable_items[_RefEq(key)] = value

    def __getitem__(self, key):
        return self.unhashable_items[_RefEq(key)]

    def __getattribute__(self, item):
        if item == "unhashable_items":
            return super().__getattribute__(item)
        return self.unhashable_items.__getattribute__(item)


class _DocumentsByEntityHolder(object):
    def __init__(self):
        self._hashable_items = dict()
        self._unhashable_items = _RefEqEntityHolder()

    def __repr__(self):
        return f"{self.__class__.__name__}: {[item for item in self.__iter__()]}"

    def __len__(self):
        return len(self._hashable_items) + len(self._unhashable_items)

    def __contains__(self, item):
        try:
            return item in self._hashable_items
        except TypeError as e:
            if str(e.args[0]).startswith("unhashable type"):
                return item in self._unhashable_items
            raise e

    def __setitem__(self, key, value):
        try:
            self._hashable_items[key] = value
        except TypeError as e:
            if str(e.args[0]).startswith("unhashable type"):
                self._unhashable_items[key] = value
                return
            raise e

    def __getitem__(self, key):
        try:
            return self._hashable_items[key]
        except (TypeError, KeyError):
            return self._unhashable_items[key]

    def __iter__(self):
        d = list(map(lambda x: x.ref, self._unhashable_items.keys()))
        if len(self._hashable_items) > 0:
            d.extend(self._hashable_items.keys())
        return (item for item in d)

    def get(self, key, default=None):
        return self[key] if key in self else default

    # calling pop on this class can cause differences between documents_by_id and documents_by_entity
    def pop(self, key, default_value=None):
        result = self._hashable_items.pop(key, None)
        if result is not None:
            return result
        return self._unhashable_items.pop(_RefEq(key), default_value)

    def clear(self):
        self._hashable_items.clear()
        self._unhashable_items.clear()

    def items(self):
        return list(
            chain(
                map(lambda item: (item[0], item[1]), self._hashable_items.items()),
                map(lambda item: (item[0].ref, item[1]), self._unhashable_items.items()),
            )
        )


class _DeletedEntitiesHolder(MutableSet):
    def __init__(self, items=None):
        if items is None:
            items = []
        self.items = set(map(_RefEq, items))

    def __getattribute__(self, item):
        if item in ["add", "discard", "items"]:
            return super().__getattribute__(item)
        return self.items.__getattribute__(item)

    def __contains__(self, item: object) -> bool:
        return _RefEq(item) in self.items

    def __len__(self) -> int:
        return len(self.items)

    def __iter__(self):
        return (item.ref for item in self.items)

    def add(self, element: object) -> None:
        return self.items.add(_RefEq(element))

    def discard(self, element: object) -> None:
        return self.items.discard(_RefEq(element))


class DocumentSession(object):
    def __init__(self, database, document_store, requests_executor, session_id, **kwargs):
        """
        Implements Unit of Work for accessing the RavenDB server

        @param str database: the name of the database we open a session to
        @param DocumentStore document_store: the store that we work on
        """
        self.session_id = session_id
        self.database = database
        self._document_store = document_store
        self._requests_executor = requests_executor
        self._documents_by_id = {}
        self._included_documents_by_id = {}
        self._deleted_entities = _DeletedEntitiesHolder()
        self._documents_by_entity = _DocumentsByEntityHolder()
        self._timeseries_defer_commands = {}
        self._time_series_by_document_id = {}
        self._counters_defer_commands = {}
        self._counters_by_document_id = {}
        self._included_counters_by_document_id = {}
        self._known_missing_ids = set()
        self.id_value = None
        self._defer_commands = set()
        self._number_of_requests_in_session = 0
        self._query = None
        self.use_optimistic_concurrency = kwargs.get("use_optimistic_concurrency", False)
        self.no_tracking = kwargs.get("no_tracking", False)
        self.no_caching = kwargs.get("no_caching", False)
        self.advanced = Advanced(self)
        self._events = None
        self.entity_to_json = EntityToJson(self)

    @property
    def readonly_events(self):
        if self._events is None:
            return self._document_store.events.clone()
        return self._events

    @property
    def events(self):
        if self._events is None:
            self._events = self._document_store.events.clone()
        return self._events

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @property
    def requests_executor(self):
        return self._requests_executor

    @property
    def number_of_requests_in_session(self):
        return self._number_of_requests_in_session

    @property
    def documents_by_entity(self):
        return self._documents_by_entity

    @property
    def deleted_entities(self):
        return self._deleted_entities

    @property
    def documents_by_id(self):
        return self._documents_by_id

    @property
    def time_series_by_document_id(
        self,
    ) -> Dict[str, Dict[str, List[TimeSeriesRangeResult]]]:
        return self._time_series_by_document_id

    @property
    def counters_by_document_id(self):
        return self._counters_by_document_id

    @property
    def included_counters_by_document_id(self):
        return self._included_counters_by_document_id

    def defer(self, command, *args):
        self._defer_commands.add(command)
        for arg in args:
            self._defer_commands.add(arg)

    @property
    def defer_commands(self):
        return self._defer_commands

    @property
    def counters_defer_commands(self):
        return self._counters_defer_commands

    @property
    def timeseries_defer_commands(self):
        return self._timeseries_defer_commands

    @property
    def known_missing_ids(self):
        return self._known_missing_ids

    @property
    def included_documents_by_id(self):
        return self._included_documents_by_id

    @property
    def conventions(self):
        return self._document_store.conventions

    @property
    def query(self):
        if self._query is None:
            self._query = Query(self)
        return self._query

    def raw_query(self, raw_query_string: str, query_parameters: dict = None, **kwargs):
        self._query = Query(self)(**kwargs)
        return self._query.raw_query(raw_query_string, query_parameters)

    def save_includes(self, includes=None):
        if includes:
            for key, value in includes.items():
                if key not in self._documents_by_id:
                    self._included_documents_by_id[key] = value

    def register_counters(self, result_counters: dict, counters_to_include: Dict[str, List[str]]):
        if self.no_tracking:
            return
        if not result_counters:
            self.__set_got_all_in_cache_if_needed(counters_to_include)
        else:
            self._register_counters_internal(result_counters, counters_to_include, True, False)
        self.__register_missing_counters(counters_to_include)

    def _register_counters_internal(
        self, result_counters: Dict, counters_to_include: Dict[str, List[str]], from_query_result: bool, got_all: bool
    ):
        for key, result_counters in result_counters.items():
            if not result_counters:
                continue
            counters = []
            if from_query_result:
                counters = counters_to_include.get(key, None)
                got_all = True if counters is not None and len(counters) == 0 else False

            if len(result_counters) == 0 and not got_all:
                cache = self.counters_by_document_id.get(key, None)
                if not cache:
                    continue
                for counter in counters:
                    del cache[1][counter]
                self.counters_by_document_id[key] = cache
                continue
            self.__register_counters_for_document(key, got_all, result_counters, counters_to_include)

    def __register_counters_for_document(
        self, key: str, got_all: bool, result_counters: List[Dict], counters_to_include: Dict[str, List[str]]
    ):
        cache = self.counters_by_document_id.get(key)
        if not cache:
            cache = [got_all, CaseInsensitiveDict()]

        deleted_counters = (
            set()
            if len(cache[1]) == 0
            else (set(cache[1].keys()) if len(counters_to_include.get(key)) == 0 else set(counters_to_include.get(key)))
        )

        for counter in result_counters:
            if counter:
                counter_name = counter["CounterName"]
                total_value = counter["TotalValue"]
            else:
                counter_name = total_value = None
            if counter_name and total_value:
                cache[1][counter_name] = total_value
                deleted_counters.discard(counter_name)

        if deleted_counters:
            for name in deleted_counters:
                del cache[1][name]

        cache[0] = got_all
        self.counters_by_document_id[key] = cache

    def __set_got_all_in_cache_if_needed(self, counters_to_include: Dict[str, List[str]]):
        if not counters_to_include:
            return
        for key, value in counters_to_include:
            if len(value) > 0:
                continue
            self.__set_got_all_counters_for_document(key)

    def __set_got_all_counters_for_document(self, key: str):
        cache = self.counters_by_document_id.get(key, None)
        if not cache:
            cache = [False, CaseInsensitiveDict()]
        cache[0] = True
        self.counters_by_document_id[key] = cache

    def __register_missing_counters(self, counters_to_include: Dict[str, List[str]]):
        if not counters_to_include:
            return

        for key, value in counters_to_include.items():
            cache = self.counters_by_document_id.get(key, None)
            if cache is None:
                cache = [False, CaseInsensitiveDict()]
                self.counters_by_document_id[key] = cache

            for counter in value:
                if counter in cache[1]:
                    continue
                cache[1][counter] = None

    def save_entity(
        self,
        key,
        entity,
        metadata,
        original_document,
        force_concurrency_check=False,
        is_new_document=False,
    ):
        if key is not None:
            self._known_missing_ids.discard(key)

            if key not in self._documents_by_id:
                self._documents_by_id[key] = entity

                self._documents_by_entity[self._documents_by_id[key]] = {
                    "original_value": original_document,
                    "metadata": metadata,
                    "change_vector": metadata.get("change_vector", None),
                    "entity": entity,
                    "is_new_document": is_new_document,
                    "key": key,
                    "force_concurrency_check": force_concurrency_check,
                }

    def _convert_and_save_entity(self, key, document, object_type, nested_object_types, is_new_document=False):
        if key not in self._documents_by_id:
            (entity, metadata, original_document,) = Utils.convert_to_entity(
                document,
                object_type,
                self.conventions,
                self.readonly_events,
                nested_object_types,
            )
            self.save_entity(key, entity, metadata, original_document, is_new_document)

    def _multi_load(self, keys, object_type, includes, nested_object_types):
        if len(keys) == 0:
            return []
        keys = list(filter(lambda x: x is not None, keys))
        ids_of_not_existing_object = set(keys)
        if not includes:
            ids_in_includes = [key for key in ids_of_not_existing_object if key in self._included_documents_by_id]
            if len(ids_in_includes) > 0:
                for include in ids_in_includes:
                    self._convert_and_save_entity(
                        include,
                        self._included_documents_by_id[include],
                        object_type,
                        nested_object_types,
                    )
                    self._included_documents_by_id.pop(include)

            ids_of_not_existing_object = [key for key in ids_of_not_existing_object if key not in self._documents_by_id]

        ids_of_not_existing_object = [key for key in ids_of_not_existing_object if key not in self._known_missing_ids]

        if len(ids_of_not_existing_object) > 0:
            self.increment_requests_count()
            command = GetDocumentCommand(ids_of_not_existing_object, includes)
            response = self._requests_executor.execute(command)
            if response:
                results = response["Results"]
                includes = response["Includes"]
                for i in range(0, len(results)):
                    if results[i] is None:
                        self._known_missing_ids.add(ids_of_not_existing_object[i])
                        continue
                    self._convert_and_save_entity(
                        ids_of_not_existing_object[i],
                        results[i],
                        object_type,
                        nested_object_types,
                    )
                self.save_includes(includes)
        return [
            None
            if key in self._known_missing_ids
            else self._documents_by_id[key]
            if key in self._documents_by_id
            else None
            for key in keys
        ]

    def load(self, key_or_keys, object_type=None, includes=None, nested_object_types=None):
        """
        @param key_or_keys: Identifier of a document that will be loaded.
        :type str or list
        @param includes: The path to a reference inside the loaded documents can be list (property name)
        :type list or str
        @param object_type: The class we want to get
        :type classObj:
        @param nested_object_types: A dict of classes for nested object the key will be the name of the class and the
         value will be the object we want to get for that attribute
        :type str
        @return: instance of object_type or None if document with given Id does not exist.
        :rtype:object_type or None
        """
        if not key_or_keys:
            return None

        if includes and not isinstance(includes, list):
            includes = [includes]

        if isinstance(key_or_keys, list):
            return self._multi_load(key_or_keys, object_type, includes, nested_object_types)

        if key_or_keys in self._known_missing_ids:
            return None
        if key_or_keys in self._documents_by_id and not includes:
            return self._documents_by_id[key_or_keys]

        if key_or_keys in self._included_documents_by_id:
            self._convert_and_save_entity(
                key_or_keys,
                self._included_documents_by_id[key_or_keys],
                object_type,
                nested_object_types,
            )
            self._included_documents_by_id.pop(key_or_keys)
            if not includes:
                return self._documents_by_id[key_or_keys]

        self.increment_requests_count()
        command = GetDocumentCommand(key_or_keys, includes=includes)
        response = self._requests_executor.execute(command)
        if response:
            result = response["Results"]
            includes = response["Includes"]
            if len(result) == 0 or result[0] is None:
                self._known_missing_ids.add(key_or_keys)
                return None
            self._convert_and_save_entity(key_or_keys, result[0], object_type, nested_object_types)
            self.save_includes(includes)
        return self._documents_by_id[key_or_keys] if key_or_keys in self._documents_by_id else None

    def is_loaded(self, key):
        return self.is_loaded_or_deleted(key)

    def is_loaded_or_deleted(self, key):
        document = self.documents_by_id[key]
        return document or self.is_deleted(key) or key in self.included_documents_by_id

    def is_deleted(self, key):
        return key in self.known_missing_ids

    def delete_by_entity(self, entity):
        if entity is None:
            raise ValueError("None entity is invalid")
        if entity not in self._documents_by_entity:
            raise exceptions.InvalidOperationException(
                "{0} is not associated with the session, cannot delete unknown entity instance".format(entity)
            )
        if "Raven-Read-Only" in self._documents_by_entity[entity]["metadata"]:
            raise exceptions.InvalidOperationException(
                "{0} is marked as read only and cannot be deleted".format(entity)
            )

        self._included_documents_by_id.pop(self._documents_by_entity[entity]["key"], None)
        self._included_counters_by_document_id.pop(self._documents_by_entity[entity]["key"], None)
        self._known_missing_ids.add(self._documents_by_entity[entity]["key"])
        self._deleted_entities.add(entity)

    def delete(self, key_or_entity, expected_change_vector=None):
        """
        @param str or object key_or_entity: Can be the key or the entity we like to delete
        @param str expected_change_vector: A change vector to use for concurrency checks
        :type str or object:
        """
        if key_or_entity is None:
            raise ValueError("None key is invalid")
        if not isinstance(key_or_entity, str):
            self.delete_by_entity(key_or_entity)
            return
        if key_or_entity in self._documents_by_id:
            entity = self._documents_by_id[key_or_entity]
            if self._has_change(entity):
                raise exceptions.InvalidOperationException(
                    "Can't delete changed entity using identifier. Use delete_by_entity(entity) instead."
                )
            if entity not in self._documents_by_entity:
                raise exceptions.InvalidOperationException(
                    "{0} is not associated with the session, cannot delete unknown entity instance".format(entity)
                )
            if "Raven-Read-Only" in self._documents_by_entity[entity]["metadata"]:
                raise exceptions.InvalidOperationException(
                    "{0} is marked as read only and cannot be deleted".format(entity)
                )
            self.delete_by_entity(entity)
            return
        self._known_missing_ids.add(key_or_entity)
        self._included_documents_by_id.pop(key_or_entity, None)
        self._included_counters_by_document_id.pop(key_or_entity, None)
        self.defer(commands_data.DeleteCommandData(key_or_entity, expected_change_vector))

    def assert_no_non_unique_instance(self, entity, key):
        if not (
            key is None or key.endswith("/") or key not in self._documents_by_id or self._documents_by_id[key] is entity
        ):
            raise exceptions.NonUniqueObjectException(
                "Attempted to associate a different object with id '{0}'.".format(key)
            )

    def store(self, entity, key="", change_vector=""):
        """
        @param entity: Entity that will be stored
        :type object:
        @param key: Entity will be stored under this key, (None to generate automatically)
        :type str:
        @param change_vector: Current entity change_vector, used for concurrency checks (None to skip check)
        :type str
        """

        if entity is None:
            raise ValueError("None entity value is invalid")

        force_concurrency_check = self._get_concurrency_check_mode(entity, key, change_vector)
        if entity in self._documents_by_entity:
            if change_vector:
                self._documents_by_entity[entity]["change_vector"] = change_vector
            self._documents_by_entity[entity]["force_concurrency_check"] = force_concurrency_check
            return

        if not key:
            entity_id = GenerateEntityIdOnTheClient.try_get_id_from_instance(entity)
        else:
            GenerateEntityIdOnTheClient.try_set_id_on_entity(entity, key)
            entity_id = key

        self.assert_no_non_unique_instance(entity, entity_id)

        if not entity_id:
            entity_id = self._document_store.generate_id(self.database, entity)
            GenerateEntityIdOnTheClient.try_set_id_on_entity(entity, entity_id)

        for command in self._defer_commands:
            if command.key == entity_id:
                raise exceptions.InvalidOperationException(
                    "Can't store document, there is a deferred command registered for this document in the session. "
                    "Document id: " + entity_id
                )

        if entity in self._deleted_entities:
            raise exceptions.InvalidOperationException(
                "Can't store object, it was already deleted in this session.  Document id: " + entity_id
            )

        metadata = self.conventions.build_default_metadata(entity)
        self._deleted_entities.discard(entity)
        self.save_entity(
            entity_id,
            entity,
            metadata,
            {},
            force_concurrency_check=force_concurrency_check,
            is_new_document=True,
        )

    def _get_concurrency_check_mode(self, entity, key="", change_vector=""):
        if key == "":
            return "disabled" if change_vector is None else "forced"
        if change_vector == "":
            if key is None:
                entity_key = GenerateEntityIdOnTheClient.try_get_id_from_instance(entity)
                return "forced" if entity_key is None else "auto"
            return "auto"

        return "disabled" if change_vector is None else "forced"

    def time_series_for(self, entity_or_document_id, name):
        """
        Get A time series object associated with the document
        """
        return TimeSeries(self, entity_or_document_id, name)

    def counters_for(self, entity_or_document_id):
        """
        Get A counters object associated with the document
        """
        return DocumentCounters(self, entity_or_document_id)

    def save_changes(self):
        # todo: create BatchOperation and use it here https://issues.hibernatingrhinos.com/issue/RDBC-474
        data = _SaveChangesData([], len(self._defer_commands))
        defer_commands = list(self._defer_commands)
        self._defer_commands.clear()
        self._prepare_for_delete_commands(data)
        self._prepare_for_puts_commands(data)
        data.commands.extend(defer_commands)
        if len(data.commands) == 0:
            return
        self.increment_requests_count()
        batch_command = BatchCommand(data.commands)
        batch_result = self._requests_executor.execute(batch_command)
        if batch_result is None:
            raise exceptions.InvalidOperationException(
                "Cannot call Save Changes after the document store was disposed."
            )
        self._update_batch_result(batch_result, data)

    def refresh(self, entity):
        document_info = self.advanced.get_document_info(entity)
        if not document_info:
            raise ValueError("Cannot refresh a transient instance")
        self.increment_requests_count()
        command = GetDocumentCommand([document_info["key"]])
        response = self.requests_executor.execute(command)
        self._refresh_internal(entity, response, document_info)

    def _refresh_internal(self, entity, response, document_info):
        document = response["Results"][0]
        if document is None:
            raise ValueError(f"Document {document_info['key']} no longer exists and was probably deleted")
        value = document.get(constants.Documents.Metadata.KEY)
        document_info["metadata"] = value
        if document_info.get("metadata") is not None:
            change_vector = value.get(constants.Documents.Metadata.CHANGE_VECTOR)
            document_info["change_vector"] = change_vector

        if self.documents_by_id.get(document_info["key"]) is not None and not self.no_tracking:
            self.entity_to_json.remove_from_missing(self.documents_by_id[document_info["key"]])
        document_info["entity"], _, _ = Utils.convert_to_entity(
            document, entity.__class__, self.conventions, self.events
        )
        # todo: when adding another fields to document_info in future like "document" ensure it's added also here
        # todo: when linking entity references to values in documents_by_id also need to update them here

    def _update_batch_result(self, batch_result, data):

        i = 0
        batch_result_length = len(batch_result)
        while i < batch_result_length:
            item = batch_result[i]
            if isinstance(item, dict) and "Type" in item:
                if item["Type"] == "PUT":
                    entity = data.entities[i - data.deferred_command_count]
                    if entity in self._documents_by_entity:
                        # todo: bind more info to values below https://issues.hibernatingrhinos.com/issue/RDBC-475
                        self._documents_by_id[item["@id"]] = entity
                        document_info = self._documents_by_entity[entity]
                        # todo: get_or_add_modifications
                        document_info["change_vector"] = item["@change-vector"]
                        item.pop("Type", None)
                        document_info["metadata"] = item
                        document_info["key"] = item["@id"]
                        #  todo: make it more sophisticated - possibly multi-copying every reference to single object
                        document_info["original_value"] = deepcopy(entity.__dict__)
                        document_info["is_new_document"] = False
                        self.readonly_events.after_save_change(self, item["@id"], entity)

                elif item["Type"] == "Counters":
                    cache = self._counters_by_document_id.get(item["Id"], None)
                    if not cache:
                        self._counters_by_document_id[item["Id"]] = [False, {}]
                        cache = self._counters_by_document_id[item["Id"]]

                    for counter in item["CountersDetail"]["Counters"]:
                        cache[1][counter["CounterName"]] = counter["TotalValue"]

                    self._counters_defer_commands.clear()
                elif item["Type"] == "TimeSeries":
                    self._timeseries_defer_commands.clear()
            i += 1

    def _prepare_for_delete_commands(self, data):
        keys_to_delete = []
        for entity in self._deleted_entities:
            keys_to_delete.append(self._documents_by_entity[entity]["key"])

        for key in keys_to_delete:
            change_vector = None
            if key in self._documents_by_id:
                existing_entity = self._documents_by_id[key]
                if existing_entity in self._documents_by_entity:
                    change_vector = (
                        self._documents_by_entity[existing_entity]["metadata"]["@change-vector"]
                        if self.advanced.use_optimistic_concurrency
                        else None
                    )
                self._documents_by_entity.pop(existing_entity, None)
                self._documents_by_id.pop(key, None)
                data.entities.append(existing_entity)
            data.commands.append(commands_data.DeleteCommandData(key, change_vector))
        self._deleted_entities.clear()

    def get_changes_from_deleted(self, changes):
        if changes is not None:
            for key in list(map(lambda entity: self.advanced.get_document_info(entity)["key"], self._deleted_entities)):
                doc_changes = []
                change = {}
                change.update({"new_value": ""})
                change.update({"old_value": ""})
                change.update({"change": "document_deleted"})
                doc_changes.append(change)
                changes.update({key: doc_changes})

    def _prepare_for_puts_commands(self, data):
        for entity in self._documents_by_entity:
            dirty = self.advanced.update_metadata_modifications(entity)
            if self._has_change(entity) or dirty:
                key = self._documents_by_entity[entity]["key"]
                change_vector = None
                if (
                    self.advanced.use_optimistic_concurrency
                    and self._documents_by_entity[entity]["force_concurrency_check"] != "disabled"
                    or self._documents_by_entity[entity]["force_concurrency_check"] == "forced"
                ):
                    change_vector = self._documents_by_entity[entity]["change_vector"]
                self.readonly_events.before_store(self, key, entity)
                data.entities.append(entity)
                if key:
                    self._documents_by_id.pop(key)
                    document = entity.__dict__.copy()
                    document.pop("Id", None)
                metadata = self._documents_by_entity[entity]["metadata"]
                data.commands.append(commands_data.PutCommandData(key, change_vector, document, metadata))

    def _has_change(self, entity):
        import json

        entity_to_dict = json.loads(json.dumps(entity, default=self.conventions.json_default_method))
        if self.advanced.get_document_info(entity)[
            "original_value"
        ] != entity_to_dict or self.advanced.get_document_info(entity).get("metadata_instance"):
            return True
        return False

    def increment_requests_count(self):
        self._number_of_requests_in_session += 1
        if self._number_of_requests_in_session > self.conventions.max_number_of_requests_per_session:
            raise exceptions.InvalidOperationException(
                "The maximum number of requests ({0}) allowed for this session has been reached. Raven limits the number \
                of remote calls that a session is allowed to make as an early warning system. Sessions are expected to \
                be short lived, and Raven provides facilities like batch saves (call save_changes() only once).\
                You can increase the limit by setting DocumentConventions.\
                MaxNumberOfRequestsPerSession or MaxNumberOfRequestsPerSession, but it is advisable \
                that you'll look into reducing the number of remote calls first, \
                since that will speed up your application significantly and result in a\
                more responsive application.".format(
                    self.conventions.max_number_of_requests_per_session
                )
            )

    def entity_to_dict(self, entity):
        return json.loads(json.dumps(entity, default=self.conventions.json_default_method))


class Advanced(object):
    def __init__(self, session):
        self.session = session
        self.use_optimistic_concurrency = (
            session.use_optimistic_concurrency
            if session.use_optimistic_concurrency
            else session.conventions.use_optimistic_concurrency
        )
        self._attachment = None

    @property
    def attachment(self):
        if not self._attachment:
            self._attachment = _Attachment(self.session)
        return self._attachment

    def has_changed(self, entity):
        return self.session._has_change(entity) or entity in self.session.deleted_entities

    def has_changes(self):
        for key, value in self.session.documents_by_entity.items():
            doc = self.session.entity_to_json.convert_entity_to_json(key, value)
            if self.entity_changed(doc, self.get_document_info(key), None):
                return True
        return len(self.session.deleted_entities) != 0

    def exists(self, key):
        if key is None:
            raise ValueError("Key cannot be None")
        if key in self.session.known_missing_ids:
            return False
        if key in self.session.documents_by_id.values():
            return True
        command = HeadDocumentCommand(key, None)
        response = self.session.requests_executor.execute(command)
        return response is not None

    def what_changed(self):
        changes = {}
        self.get_all_entities_changes(changes)
        self.session.get_changes_from_deleted(changes)
        return changes

    def get_all_entities_changes(self, changes):
        for key, document in self.session.documents_by_id.items():
            self.update_metadata_modifications(document)
            new_obj = self.session.entity_to_json.convert_entity_to_json(document, self.get_document_info(document))
            self.entity_changed(new_obj, self.get_document_info(document), changes)

    def entity_changed(self, new_obj, document_info, changes):
        return JsonOperation.entity_changed(
            new_obj, document_info, changes, self.session.conventions.json_default_method
        )

    def get_metadata_for(self, entity):
        if entity is None:
            raise ValueError("Entity cannot be None")
        document_info = self.get_document_info(entity)
        if document_info.get("metadata_instance"):
            return document_info["metadata_instance"]
        metadata_as_json = document_info["metadata"]
        metadata_obj = MetadataAsDictionary(metadata=metadata_as_json)
        document_info["metadata_instance"] = metadata_obj
        return metadata_obj

    def get_document_info(self, entity):
        return self.session.documents_by_entity.get(entity, None)

    def update_metadata_modifications(self, document):
        dirty = False
        metadata_instance = self.get_document_info(document).get("metadata_instance")
        if metadata_instance is not None:
            metadata = self.get_document_info(document)["metadata"]
            if metadata_instance.is_dirty is True:
                dirty = True
            for key, value in metadata_instance.items():
                if value is None or isinstance(value, MetadataAsDictionary) and value.is_dirty is True:
                    dirty = True
                metadata.update(
                    {key: json.loads(json.dumps(value, default=self.session.conventions.json_default_method))}
                )
        return dirty

    def stream(self, query):
        from pyravendb.store.stream import IncrementalJsonParser
        import ijson

        index_query = query.get_index_query()
        if index_query.wait_for_non_stale_results:
            raise exceptions.NotSupportedException(
                "Since stream() does not wait for indexing (by design), "
                "streaming query with wait_for_non_stale_results is not supported."
            )
        self.session.increment_requests_count()
        command = QueryStreamCommand(index_query)
        with self.session.requests_executor.execute(command) as response:
            basic_parse = IncrementalJsonParser.basic_parse(response)
            parser = ijson.backend.common.parse(basic_parse)

            results = ijson.backend.common.items(parser, "Results")
            for result in next(results, None):
                document, metadata, _ = Utils.convert_to_entity(
                    result,
                    query.object_type,
                    self.session.conventions,
                    self.session.readonly_events,
                    query.nested_object_types,
                )
                yield {
                    "document": document,
                    "metadata": metadata,
                    "id": metadata.get("@id", None),
                    "change-vector": metadata.get("@change-vector", None),
                }

    def number_of_requests_in_session(self):
        return self.session.number_of_requests_in_session

    def get_document_id(self, instance):
        if instance is not None:
            if instance in self.session.documents_by_entity:
                return self.session.documents_by_entity[instance]["key"]
        return None

    def get_document_by_id(self, key):
        if key:
            return self.get_document_info(self.session.documents_by_id.get(key, None))

    @property
    def document_store(self):
        """
        The document store associated with this session
        """
        return self.session._document_store

    def clear(self):
        self.session.documents_by_entity.clear()
        self.session.documents_by_id.clear()
        self.session.deleted_entities.clear()
        self.session.included_documents_by_id.clear()
        self.session.known_missing_ids.clear()


class _Attachment:
    def __init__(self, session):
        self._session = session
        self._store = session.advanced.document_store
        self._defer_commands = session.defer_commands

    def _command_exists(self, document_id):
        for command in self._defer_commands:
            if not isinstance(
                command,
                (
                    PutAttachmentCommandData,
                    DeleteAttachmentCommandData,
                    DeleteCommandData,
                ),
            ):
                continue

            if command.key == document_id:
                return command

    def throw_not_in_session(self, entity):
        raise ValueError(
            repr(entity) + " is not associated with the session, cannot add attachment to it. "
            "Use document Id instead or track the entity in the session."
        )

    def store(self, entity_or_document_id, name, stream, content_type=None, change_vector=None):
        if not isinstance(entity_or_document_id, str):
            entity = self._session.documents_by_entity.get(entity_or_document_id, None)
            if not entity:
                self.throw_not_in_session(entity_or_document_id)
            entity_or_document_id = entity["metadata"]["@id"]

        if not entity_or_document_id:
            raise ValueError(entity_or_document_id)
        if not name:
            raise ValueError(name)

        defer_command = self._command_exists(entity_or_document_id)
        if defer_command:
            message = "Can't store attachment {0} of document {1}".format(name, entity_or_document_id)
            if defer_command.type == "DELETE":
                message += ", there is a deferred command registered for this document to be deleted."
            elif defer_command.type == "AttachmentPUT":
                message += ", there is a deferred command registered to create an attachment with the same name."
            elif defer_command.type == "AttachmentDELETE":
                message += ", there is a deferred command registered to delete an attachment with the same name."
            raise exceptions.InvalidOperationException(message)

        entity = self._session.documents_by_id.get(entity_or_document_id, None)
        if entity and entity in self._session.deleted_entities:
            raise exceptions.InvalidOperationException(
                "Can't store attachment "
                + name
                + " of document"
                + entity_or_document_id
                + ", the document was already deleted in this session."
            )

        self._session.defer(PutAttachmentCommandData(entity_or_document_id, name, stream, content_type, change_vector))

    def delete(self, entity_or_document_id, name):
        if not isinstance(entity_or_document_id, str):
            entity = self._session.documents_by_entity.get(entity_or_document_id, None)
            if not entity:
                self.throw_not_in_session(entity_or_document_id)
            entity_or_document_id = entity["metadata"]["@id"]

        if not entity_or_document_id:
            raise ValueError(entity_or_document_id)
        if not name:
            raise ValueError(name)

        defer_command = self._command_exists(entity_or_document_id)
        if defer_command:
            if defer_command.type in ("Delete", "AttachmentDELETE"):
                return
            if defer_command.type == "AttachmentPUT":
                raise exceptions.InvalidOperationException(
                    "Can't delete attachment "
                    + name
                    + " of document "
                    + entity_or_document_id
                    + ",there is a deferred command registered to create an attachment with the same name."
                )

        entity = self._session.documents_by_id.get(entity_or_document_id, None)
        if entity and entity in self._session.deleted_entities:
            return

        self._session.defer(DeleteAttachmentCommandData(entity_or_document_id, name, None))

    def get(self, entity_or_document_id, name):
        if not isinstance(entity_or_document_id, str):
            entity = self._session.documents_by_entity.get(entity_or_document_id, None)
            if not entity:
                self.throw_not_in_session(entity_or_document_id)
            entity_or_document_id = entity["metadata"]["@id"]

        operation = GetAttachmentOperation(entity_or_document_id, name, AttachmentType.document, None)
        return self._store.operations.send(operation)
